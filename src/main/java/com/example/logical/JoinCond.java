package com.example.logical;


public class JoinCond {
    int left;
    int right;
    int type;

    public JoinCond(int left, int right, int type) {
        this.left = left;
        this.right = right;
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public int getLeft() {
        return left;
    }

    public int getRight() {
        return right;
    }
}
