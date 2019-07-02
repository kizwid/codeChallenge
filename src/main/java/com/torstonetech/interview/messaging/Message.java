package com.torstonetech.interview.messaging;

public interface Message {
  /**
   * @return The underlying message payload.
   */
  byte[] getMsg();

  /**
   * Dispose of any resources held by this message. This should be called by the
   * application once it has finished processing the message.
   */
  void dispose();
}
