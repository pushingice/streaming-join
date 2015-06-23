package com.github.pushingice;

import java.util.Properties;
import java.util.Random;

public class Message {

    private long id;
    private byte[] content;

    public Message(Properties props, Random random) {
        id = random.nextLong();
        content = new byte[Integer.parseInt(
                props.getProperty(Constants.CONFIG_MESSAGE_BYTES))];
        random.nextBytes(content);
    }

    public Long getId() {
        return id;
    }

    public byte[] getContent() {
        return content;
    }

}
