use super::*;

#[tokio::test]
async fn queue_reservation_wakes_on_connection_close_without_closing_shared_nic() {
    let device = Arc::new(Semaphore::new(2));
    let credits = ReadCredits::new(device.clone(), 1);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    sender.try_send(1).unwrap();
    let acquiring = reserve_send(&sender, None, || credits.closed());
    tokio::pin!(acquiring);
    assert!(futures_util::poll!(&mut acquiring).is_pending());

    credits.close();
    assert_eq!(
        acquiring.await.unwrap_err().kind,
        ErrorKind::ConnectionClosed
    );
    assert_eq!(receiver.try_recv().unwrap(), 1);
    assert!(receiver.try_recv().is_err());
    assert!(!device.is_closed());
    assert_eq!(device.available_permits(), 2);
}

#[tokio::test]
async fn queue_reservation_observes_connection_closed_before_first_poll() {
    let credits = ReadCredits::new(Arc::new(Semaphore::new(2)), 1);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    sender.try_send(1).unwrap();
    let acquiring = reserve_send(&sender, None, || credits.closed());
    credits.close();

    assert_eq!(
        acquiring.await.unwrap_err().kind,
        ErrorKind::ConnectionClosed
    );
    assert_eq!(receiver.try_recv().unwrap(), 1);
    assert!(receiver.try_recv().is_err());
}
