package com.torstonetech.interview.messaging;

public class MessagingException extends Exception {
  public MessagingException(final Throwable cause) {
    super(cause);
  }

  public MessagingException(final String message) {
    super(message);
  }

  public MessagingException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
