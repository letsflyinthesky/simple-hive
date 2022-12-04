package com.example;

import org.antlr.runtime.TokenRewriteStream;


public class Context {

    private String command;
    private TokenRewriteStream tokenRewriteStream;

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public TokenRewriteStream getTokenRewriteStream() {
        return tokenRewriteStream;
    }

    public void setTokenRewriteStream(TokenRewriteStream tokenRewriteStream) {
        this.tokenRewriteStream = tokenRewriteStream;
    }
}
