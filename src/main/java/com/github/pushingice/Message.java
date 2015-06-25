package com.github.pushingice;

import java.util.Properties;
import java.util.Random;

public class Message {

    private String messageType;
    private String fkMessageType = "";
    private long fkId = 0;
    private long id;
    private byte[] content;

    public Message(Properties props, Random random, String messageType) {
        this.messageType = messageType;
        id = random.nextLong();
        content = new byte[Integer.parseInt(
                props.getProperty(Constants.CONFIG_MESSAGE_BYTES))];
        random.nextBytes(content);
    }

    public Message(String messageType, long id, String fkMessageType,
                   long fkId, byte[] content) {
        this.messageType = messageType;
        this.id = id;
        this.content = content;
        this.fkMessageType = fkMessageType;
        this.fkId = fkId;
    }


    public Message copy() {
        return new Message(messageType, id, fkMessageType, fkId, content);
    }

    public Long getId() {
        return id;
    }

    public byte[] getContent() {
        return content;
    }

    public String getMessageType() {
        return messageType;
    }

    public String getFkMessageType() {
        return fkMessageType;
    }

    public long getFkId() {
        return fkId;
    }

    public void setFkMessageType(String fkMessageType) {
        this.fkMessageType = fkMessageType;
    }

    public void setFkId(long fkId) {
        this.fkId = fkId;
    }


    @Override
    public String toString() {
        return "Message{" +
                "messageType='" + messageType + '\'' +
                ", id=" + id +
                ", fkMessageType='" + fkMessageType + '\'' +
                ", fkId=" + fkId +
                ", contentHash=" + content.hashCode() +
                '}';
    }
}
