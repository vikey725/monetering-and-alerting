package plugins;

import app.Operator;

public class GtOperator implements Operator {
    public String name() { return "gt"; }
    public boolean test(int l, int r) { return l > r; }
}
