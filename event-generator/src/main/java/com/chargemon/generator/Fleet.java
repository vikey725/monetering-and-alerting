package com.chargemon.generator;

import com.chargemon.generator.scenario.Scenario;
import com.chargemon.generator.scenario.Scenarios;
import com.chargemon.ocpp.model.OcppVersion;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** The simulated stations plus a small three-level group hierarchy (country > region > site). */
record Fleet(List<StationSim> stations, Map<String, String> groups) {

    private static final String[] COUNTRIES = {"de", "fr", "us"};
    private static final String[] VENDORS = {"ACME", "VoltCo", "ChargeMax"};

    static Fleet build(int count, List<String> profiles, double v16Ratio, long seed) {
        Random rnd = new Random(seed);
        Map<String, String> groups = new LinkedHashMap<>();
        List<String> sites = new ArrayList<>();
        for (String c : COUNTRIES) {
            groups.put("country:" + c, null);
            for (int r = 1; r <= 2; r++) {
                String region = "region:" + c + r;
                groups.put(region, "country:" + c);
                for (int s = 1; s <= 3; s++) {
                    String site = "site:" + c + r + "-" + s;
                    groups.put(site, region);
                    sites.add(site);
                }
            }
        }
        List<StationSim> stations = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            OcppVersion v = rnd.nextDouble() < v16Ratio ? OcppVersion.V16 : OcppVersion.V201;
            Scenario scenario = Scenarios.byName(profiles.get(i % profiles.size()));
            stations.add(new StationSim(String.format("ST-%06d", i), v, VENDORS[i % VENDORS.length], "M" + (i % 4),
                    sites.get(i % sites.size()), scenario, rnd.nextLong()));
        }
        return new Fleet(stations, groups);
    }
}
