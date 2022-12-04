package com.example.parse;

import java.util.List;


public interface Node {

    List<? extends Node> getChildren();

    String getName();
}
