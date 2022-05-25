package com.mq.kafkaconsumer;

public class MessageBody {
    private String title;
    private String message;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "MessageBody{" +
                "title='" + title + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
