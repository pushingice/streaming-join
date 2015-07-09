package com.github.pushingice;

import java.util.Comparator;

public class MessageComparator implements Comparator<Message> {

    @Override
    public int compare(Message a, Message b) {
        if (!a.getMessageType().equals(b.getMessageType())) {
            return a.getMessageType().compareTo(b.getMessageType());
        } else if (a.getId() != b.getId()) {
            return (a.getId() < b.getId()) ? -1 : 1;
        } else if (!a.getFkMessageType().equals(b.getFkMessageType())) {
            return a.getFkMessageType().compareTo(b.getFkMessageType());
        } else if (a.getFkId() != b.getFkId()) {
            return (a.getFkId() < b.getFkId()) ? -1 : 1;
        } else {
            return 0;
        }
    }
}
