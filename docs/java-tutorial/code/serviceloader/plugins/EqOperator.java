package plugins;

import app.Operator;

// A plugin. Public class, public no-argument constructor (the default one is fine).
public class EqOperator implements Operator {
    public String name() { return "eq"; }
    public boolean test(int l, int r) { return l == r; }
}
