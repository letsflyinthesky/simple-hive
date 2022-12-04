package com.example.logical.evaluator;

import com.example.logical.objectparser.ObjectParser;
import com.google.inject.internal.cglib.core.$AbstractClassGenerator;

/**
 * @author zhishui
 */
public abstract class GenericEvaluator {


    public abstract Object eval(Object... params);

    public abstract ObjectParser initialize(ObjectParser objectParser);


}
