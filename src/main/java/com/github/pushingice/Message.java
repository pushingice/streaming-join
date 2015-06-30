package com.github.pushingice;

import java.util.Base64;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class Message {

    private String messageType;
    private String fkMessageType = "";
    private UUID fkId = null;
    private UUID id;
    private long timestamp;
    private String crudType = Constants.CREATE;
    private String content;

    public Message(Properties props, Random random, String messageType) {
        this.messageType = messageType;
        id = UUID.randomUUID();
        byte[] rawContent = new byte[Integer.parseInt(
                props.getProperty(Constants.CONFIG_MESSAGE_BYTES))];
        random.nextBytes(rawContent);
        timestamp = System.nanoTime();
        content = Base64.getEncoder().encodeToString(rawContent);
    }

    public Message(String messageType, UUID id, String fkMessageType,
                   UUID fkId, String content) {
        this.messageType = messageType;
        this.id = id;
        this.content = content;
        this.fkMessageType = fkMessageType;
        this.fkId = fkId;
        this.timestamp = System.nanoTime();
    }


    public Message copy() {
        return new Message(messageType, id, fkMessageType, fkId, content);
    }

    public UUID getId() {
        return id;
    }

    public String getContent() {
        return content;
    }

    public String getMessageType() {
        return messageType;
    }

    public String getFkMessageType() {
        return fkMessageType;
    }

    public UUID getFkId() {
        return fkId;
    }

    public void setFkMessageType(String fkMessageType) {
        this.fkMessageType = fkMessageType;
    }

    public void setFkId(UUID fkId) {
        this.fkId = fkId;
    }

    public void setCrudType(String crudType) {
        this.crudType = crudType;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageType='" + messageType + "'" +
                ", id=" + id +
                ", fkMessageType='" + fkMessageType + "'" +
                ", fkId=" + fkId +
                ", ts=" + timestamp +
                ", crud='" + crudType + "'" +
                '}';
    }

}
