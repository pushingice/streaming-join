package com.github.pushingice;

import java.util.Base64;
import java.util.Properties;
import java.util.Random;

public class Message {

    private String messageType;
    private String fkMessageType = "";
    private long fkId = 0;
    private long id;
    private long timestamp;
    private String crudType = Constants.CREATE;
    private String content;

    public Message(Properties props, Random random, String messageType) {
        this.messageType = messageType;
        id = IDCounter.next();
        byte[] rawContent = new byte[random.nextInt(Integer.parseInt(
                props.getProperty(Constants.CONFIG_MESSAGE_BYTES)))];
        random.nextBytes(rawContent);
        timestamp = System.nanoTime();
        content = Base64.getEncoder().encodeToString(rawContent);
    }

    public Message(String messageType, long id, String fkMessageType,
                   long fkId, String content) {
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

    public long getId() {
        return id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
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

    public void setCrudType(String crudType) {
        this.crudType = crudType;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        String shortContent;
        if (content.length() > 8) {
            shortContent = content.substring(0, 4) + ".." +
                    content.substring(content.length()-4, content.length());
        } else {
            shortContent = content;
        }
        return "Message{" +
                "messageType='" + messageType + "'" +
                ", id=" + id +
                ", fkMessageType='" + fkMessageType + "'" +
                ", fkId=" + fkId +
                ", ts=" + timestamp +
                ", crud='" + crudType + "'" +
                ", content='" + shortContent + "'" +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Message message = (Message) o;

        if (fkId != message.fkId) return false;
        if (id != message.id) return false;
        if (!content.equals(message.content)) return false;
        if (!crudType.equals(message.crudType)) return false;
        if (!fkMessageType.equals(message.fkMessageType)) return false;
        if (!messageType.equals(message.messageType)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = messageType.hashCode();
        result = 31 * result + fkMessageType.hashCode();
        result = 31 * result + (int) (fkId ^ (fkId >>> 32));
        result = 31 * result + (int) (id ^ (id >>> 32));
        result = 31 * result + crudType.hashCode();
        result = 31 * result + content.hashCode();
        return result;
    }
}
