package org.example.ym.concbuff;


import java.util.Objects;

public final class Message {
    private static final int CLIENT_ERR_CODE_LOWER = 400;
    private static final int CLIENT_ERR_CODE_UPPER = 499;

    private static final int SERVER_ERR_CODE_LOWER = 500;
    private static final int SERVER_ERR_CODE_UPPER = 599;


    private final String agentString;
    private final int responseCode;

    public Message(String agentString, int responseCode) {
        this.agentString = agentString;
        this.responseCode = responseCode;
    }

    public String getAgentString() {
        return agentString;
    }

    public int getResponseCode() {
        return responseCode;
    }


    public boolean isError() {
        return isClientError() || isServerError();
    }

    public boolean isClientError() {
        return responseCode >= CLIENT_ERR_CODE_LOWER && responseCode <= CLIENT_ERR_CODE_UPPER;
    }

    public boolean isServerError() {
        return responseCode >= SERVER_ERR_CODE_LOWER && responseCode <= SERVER_ERR_CODE_UPPER;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message)) return false;
        Message message = (Message) o;
        return responseCode == message.responseCode &&
                Objects.equals(agentString, message.agentString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(agentString, responseCode);
    }
}
