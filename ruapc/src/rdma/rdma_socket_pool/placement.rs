use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum PathClass {
    InfiniBand,
    RoceV2,
    RoceOther,
}

pub(super) struct Candidate<'a> {
    pub(super) index: usize,
    pub(super) local_index: usize,
    pub(super) remote: &'a str,
    pub(super) same_zone: bool,
    pub(super) class: PathClass,
    pub(super) blacklisted: bool,
    pub(super) local_load: u64,
    pub(super) remote_load: u64,
}

pub(super) struct Selection<'a> {
    pub(super) required_remote: Option<&'a str>,
    pub(super) avoided_remotes: &'a HashSet<String>,
}

pub(super) fn choose_path(
    candidates: &[Candidate<'_>],
    selection: Selection<'_>,
    draws: [u64; 2],
) -> Option<usize> {
    let eligible_indices = eligible_paths(candidates, &selection, true);
    let eligible: Vec<&Candidate<'_>> = candidates
        .iter()
        .filter(|candidate| eligible_indices.contains(&candidate.index))
        .collect();
    if eligible.is_empty() {
        return None;
    }

    let mut remote_loads = HashMap::<&str, u64>::new();
    let mut remotes = Vec::new();
    for candidate in &eligible {
        if !remote_loads.contains_key(candidate.remote) {
            remotes.push(candidate.remote);
            remote_loads.insert(candidate.remote, candidate.remote_load);
        }
    }
    let chosen_remote = if remotes.len() == 1 {
        remotes[0]
    } else {
        let a = draws[0] as usize % remotes.len();
        let mut b = draws[1] as usize % (remotes.len() - 1);
        if b >= a {
            b += 1;
        }
        if remote_loads[remotes[b]] < remote_loads[remotes[a]] {
            remotes[b]
        } else {
            remotes[a]
        }
    };

    eligible
        .into_iter()
        .filter(|candidate| candidate.remote == chosen_remote)
        .min_by_key(|candidate| (candidate.local_load, candidate.local_index))
        .map(|candidate| candidate.index)
}

pub(super) fn eligible_paths(
    candidates: &[Candidate<'_>],
    selection: &Selection<'_>,
    blacklist_fallback: bool,
) -> Vec<usize> {
    let mut eligible: Vec<&Candidate<'_>> = candidates
        .iter()
        .filter(|candidate| {
            selection
                .required_remote
                .is_none_or(|required| candidate.remote == required)
        })
        .collect();
    retain_if_any(&mut eligible, |candidate| {
        !selection.avoided_remotes.contains(candidate.remote)
    });
    if blacklist_fallback {
        retain_if_any(&mut eligible, |candidate| !candidate.blacklisted);
    } else {
        eligible.retain(|candidate| !candidate.blacklisted);
    }
    retain_if_any(&mut eligible, |candidate| candidate.same_zone);
    if let Some(best_class) = eligible.iter().map(|candidate| candidate.class).min() {
        eligible.retain(|candidate| candidate.class == best_class);
    }
    eligible
        .into_iter()
        .map(|candidate| candidate.index)
        .collect()
}

fn retain_if_any<T>(items: &mut Vec<T>, predicate: impl Fn(&T) -> bool) {
    if items.iter().any(&predicate) {
        items.retain(predicate);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ReconcileAction {
    ConnectCoverage(String),
    ConnectTarget,
}

pub(super) fn plan_connections(
    remote_names: &[String],
    healthy_remotes: &[String],
    coverage_blocked: &HashSet<String>,
    min_per_remote: usize,
    target: usize,
    max: usize,
) -> Vec<ReconcileAction> {
    let mut actions = Vec::new();
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for remote in healthy_remotes {
        *counts.entry(remote).or_default() += 1;
    }
    let mut total = healthy_remotes.len();
    while total < max {
        let mut progressed = false;
        for remote in remote_names {
            if total == max {
                break;
            }
            if coverage_blocked.contains(remote) {
                continue;
            }
            let count = counts.entry(remote).or_default();
            if *count < min_per_remote {
                *count += 1;
                total += 1;
                actions.push(ReconcileAction::ConnectCoverage(remote.clone()));
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
    }
    while total < target.min(max) {
        actions.push(ReconcileAction::ConnectTarget);
        total += 1;
    }
    actions
}

pub(super) struct ExistingStripe<'a> {
    pub(super) index: usize,
    pub(super) local: &'a str,
    pub(super) remote: &'a str,
    pub(super) local_load: u64,
    pub(super) remote_load: u64,
    pub(super) remote_healthy: usize,
    pub(super) remote_advertised: bool,
}

pub(super) struct Replacement<'a> {
    pub(super) index: usize,
    pub(super) local: &'a str,
    pub(super) remote: &'a str,
    pub(super) local_load: u64,
    pub(super) remote_load: u64,
}

pub(super) fn choose_rebalance(
    stripes: &[ExistingStripe<'_>],
    candidates: &[Replacement<'_>],
    min_remote_coverage: usize,
    threshold: u64,
    execute_gate: bool,
) -> Option<(usize, usize)> {
    if !execute_gate {
        return None;
    }
    let victim = stripes
        .iter()
        .filter(|stripe| !stripe.remote_advertised || stripe.remote_healthy > min_remote_coverage)
        .max_by_key(|stripe| {
            stripe.local_load.saturating_sub(1) + stripe.remote_load.saturating_sub(1)
        })?;
    let victim_score = victim.local_load.saturating_sub(1) + victim.remote_load.saturating_sub(1);
    let (replacement_score, replacement) = candidates
        .iter()
        .map(|candidate| {
            let local = candidate
                .local_load
                .saturating_sub(u64::from(candidate.local == victim.local));
            let remote = candidate
                .remote_load
                .saturating_sub(u64::from(candidate.remote == victim.remote));
            (local + remote, candidate)
        })
        .min_by_key(|(score, _)| *score)?;
    (replacement_score.saturating_add(threshold) <= victim_score)
        .then_some((victim.index, replacement.index))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soft_filters_fall_back_but_hard_constraint_does_not() {
        let avoided = HashSet::from(["a".to_owned(), "b".to_owned()]);
        let candidates = [candidate(0, "a", true), candidate(1, "b", true)];
        assert!(
            choose_path(
                &candidates,
                Selection {
                    required_remote: Some("missing"),
                    avoided_remotes: &avoided,
                },
                [0, 1],
            )
            .is_none()
        );
        assert!(
            choose_path(
                &candidates,
                Selection {
                    required_remote: None,
                    avoided_remotes: &avoided,
                },
                [0, 1],
            )
            .is_some()
        );
    }

    #[test]
    fn zone_precedes_link_class() {
        let mut ib = candidate(0, "ib", false);
        ib.class = PathClass::InfiniBand;
        let mut roce = candidate(1, "roce", false);
        roce.same_zone = true;
        assert_eq!(
            choose_path(
                &[ib, roce],
                Selection {
                    required_remote: None,
                    avoided_remotes: &HashSet::new(),
                },
                [0, 1],
            ),
            Some(1)
        );
    }

    #[test]
    fn reconcile_coverage_before_target() {
        assert_eq!(
            plan_connections(
                &["a".into(), "b".into()],
                &["a".into()],
                &HashSet::new(),
                1,
                3,
                4,
            ),
            [
                ReconcileAction::ConnectCoverage("b".into()),
                ReconcileAction::ConnectTarget,
            ]
        );
    }

    #[test]
    fn blocked_coverage_falls_back_to_target() {
        assert_eq!(
            plan_connections(
                &["a".into(), "b".into()],
                &["a".into()],
                &HashSet::from(["b".into()]),
                1,
                2,
                4,
            ),
            [ReconcileAction::ConnectTarget]
        );
    }

    #[test]
    fn rebalance_preserves_coverage_and_threshold() {
        let stripes = [ExistingStripe {
            index: 7,
            local: "l0",
            remote: "r0",
            local_load: 5,
            remote_load: 5,
            remote_healthy: 2,
            remote_advertised: true,
        }];
        let candidates = [Replacement {
            index: 3,
            local: "l1",
            remote: "r1",
            local_load: 0,
            remote_load: 0,
        }];
        assert_eq!(
            choose_rebalance(&stripes, &candidates, 1, 2, true),
            Some((7, 3))
        );
        assert_eq!(choose_rebalance(&stripes, &candidates, 2, 2, true), None);
        assert_eq!(choose_rebalance(&stripes, &candidates, 1, 20, true), None);
    }

    fn candidate(index: usize, remote: &str, blacklisted: bool) -> Candidate<'_> {
        Candidate {
            index,
            local_index: 0,
            remote,
            same_zone: false,
            class: PathClass::RoceV2,
            blacklisted,
            local_load: 0,
            remote_load: index as u64,
        }
    }
}
