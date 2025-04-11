import logging

logger = logging.getLogger(__name__)

def notify(subject, body, to_email=None):
    """
    Log a notification message.
    
    Args:
        subject (str): The subject or title of the notification.
        body (str): The message body.
        to_email (str, optional): Ignored in this refactored version.
    """
    # In the refactored approach, we're simply logging notifications.
    logger.info("Notification - %s: %s", subject, body)

# Example usage for testing purposes:
if __name__ == "__main__":
    notify("Test Subject", "This is a test message.")
