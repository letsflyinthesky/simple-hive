package com.example;

import com.example.parse.ASTNode;
import com.example.parse.SimpleLexer;
import com.example.parse.SimpleParser;
import org.antlr.runtime.*;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;


public class ParseUtil {

    public static final TreeAdaptor adaptor = new CommonTreeAdaptor() {

        @Override
        public Object create(Token payload) {
            return new ASTNode(payload);
        }

        @Override
        public Object errorNode(TokenStream input, Token start, Token stop, RecognitionException e) {
            return new ASTNode(start);
        };
    };

    public static ASTNode parse(String command, Context context) {
        SimpleLexer simpleLexer = new SimpleLexer(new ANTLRStringStream(command));
        TokenRewriteStream tokens = new TokenRewriteStream(simpleLexer);
        if (context != null) {
            context.setTokenRewriteStream(tokens);
        }
        SimpleParser simpleParser = new SimpleParser(tokens);
        simpleParser.setTreeAdaptor(adaptor);
        SimpleParser.statement_return statement = null;
        try {
            statement = simpleParser.statement();
        } catch (RecognitionException e) {
            e.printStackTrace();
        }
        ASTNode tree = statement.getTree();
        return tree;
    }

}
