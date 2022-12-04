package com.example.parse;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class ASTNode extends CommonTree implements Node, Serializable {
    private static final long serialVersionUID = 1L;

    public ASTNode(Token t) {
        super(t);
    }

    public ASTNode(ASTNode node) {
        super(node);
    }

    public String getName() {
        return String.valueOf(super.getToken().getType());
    }

    @Override
    public List<? extends Node> getChildren() {
        if (super.getChildCount() == 0) {
            return null;
        }
        List<Node> result = new ArrayList<Node>();
        for (int i = 0; i < super.getChildCount(); i++) {
            result.add((Node) super.getChild(i));
        }
        return result;
    }

}
