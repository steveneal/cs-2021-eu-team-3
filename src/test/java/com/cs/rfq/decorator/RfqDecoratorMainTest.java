package com.cs.rfq.decorator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class RfqDecoratorMainTest {

    static String[] basicArgs = {};

    @Test
    public void willNotCrashWhenRFQMainRun() {
        RfqDecoratorMain rfpMain = new RfqDecoratorMain();
        try{
            rfpMain.main(basicArgs);
        }
        catch(Exception e){
            fail(e.getMessage());
        }

    }
}