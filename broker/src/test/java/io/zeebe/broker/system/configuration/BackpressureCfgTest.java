/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.system.configuration.backpressure.BackpressureCfg;
import io.zeebe.broker.system.configuration.backpressure.BackpressureCfg.LimitAlgorithm;
import io.zeebe.broker.system.configuration.backpressure.FixedLimitCfg;
import io.zeebe.broker.system.configuration.backpressure.Gradient2Cfg;
import io.zeebe.broker.system.configuration.backpressure.GradientCfg;
import io.zeebe.broker.system.configuration.backpressure.VegasCfg;
import io.zeebe.test.util.TestConfigurationFactory;
import io.zeebe.util.Environment;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public final class BackpressureCfgTest {

  public static final String BROKER_BASE = "test";

  public final Map<String, String> environment = new HashMap<>();

  @Test
  public void shouldSetBackpressureConfig() {
    // when
    final BrokerCfg cfg = readConfig("backpressure-cfg");
    final BackpressureCfg backpressure = cfg.getBackpressure();

    // then
    assertThat(backpressure.isEnabled()).isTrue();
    assertThat(backpressure.useWindowed()).isFalse();
    assertThat(backpressure.getAlgorithm()).isEqualTo(LimitAlgorithm.GRADIENT);
  }

  @Test
  public void shouldSetAimdConfig() {
    // when
    final BrokerCfg cfg = readConfig("backpressure-cfg");
    final BackpressureCfg backpressure = cfg.getBackpressure();
    final var aimd = backpressure.getAimd();

    // then
    assertThat(aimd.getBackoffRatio()).isEqualTo(0.75);
    assertThat(aimd.getRequestTimeout()).isEqualTo(Duration.ofSeconds(5));
    assertThat(aimd.getInitialLimit()).isEqualTo(15);
    assertThat(aimd.getMinLimit()).isEqualTo(5);
    assertThat(aimd.getMaxLimit()).isEqualTo(150);
  }

  @Test
  public void shouldSetFixedLimitCfg() {
    // when
    final BrokerCfg cfg = readConfig("backpressure-cfg");
    final FixedLimitCfg fixedLimitCfg = cfg.getBackpressure().getFixedLimit();

    // then
    assertThat(fixedLimitCfg.getLimit()).isEqualTo(12);
  }

  @Test
  public void shouldSetVegasCfg() {
    // when
    final BrokerCfg cfg = readConfig("backpressure-cfg");
    final VegasCfg vegasCfg = cfg.getBackpressure().getVegas();

    // then
    assertThat(vegasCfg.getAlpha()).isEqualTo(4);
    assertThat(vegasCfg.getBeta()).isEqualTo(8);
    assertThat(vegasCfg.getInitialLimit()).isEqualTo(14);
  }

  @Test
  public void shouldSetGradientCfg() {
    // when
    final BrokerCfg cfg = readConfig("backpressure-cfg");
    final GradientCfg gradientCfg = cfg.getBackpressure().getGradient();

    // then
    assertThat(gradientCfg.getMinLimit()).isEqualTo(7);
    assertThat(gradientCfg.getInitialLimit()).isEqualTo(17);
    assertThat(gradientCfg.getRttTolerance()).isEqualTo(1.7);
  }

  @Test
  public void shouldSetGradient2Cfg() {
    // when
    final BrokerCfg cfg = readConfig("backpressure-cfg");
    final Gradient2Cfg gradient2Cfg = cfg.getBackpressure().getGradient2();
    // then
    assertThat(gradient2Cfg.getMinLimit()).isEqualTo(3);
    assertThat(gradient2Cfg.getInitialLimit()).isEqualTo(13);
    assertThat(gradient2Cfg.getRttTolerance()).isEqualTo(1.3);
    assertThat(gradient2Cfg.getLongWindow()).isEqualTo(300);
  }

  @Test
  public void shouldUseConfiguredBackpressureAlgorithms() {

    final BackpressureCfg backpressure = new BackpressureCfg();

    // when
    backpressure.setAlgorithm("gradient");
    // then
    assertThat(backpressure.getAlgorithm()).isEqualTo(LimitAlgorithm.GRADIENT);

    // when
    backpressure.setAlgorithm("gradient2");
    // then
    assertThat(backpressure.getAlgorithm()).isEqualTo(LimitAlgorithm.GRADIENT2);

    // when
    backpressure.setAlgorithm("vegas");
    // then
    assertThat(backpressure.getAlgorithm()).isEqualTo(LimitAlgorithm.VEGAS);

    // when
    backpressure.setAlgorithm("fixed");
    // then
    assertThat(backpressure.getAlgorithm()).isEqualTo(LimitAlgorithm.FIXED);

    // when
    backpressure.setAlgorithm("aimd");
    // then
    assertThat(backpressure.getAlgorithm()).isEqualTo(LimitAlgorithm.AIMD);
  }

  private BrokerCfg readConfig(final String name) {
    final String configPath = "/system/" + name + ".yaml";

    final Environment environmentVariables = new Environment(environment);

    final BrokerCfg config =
        new TestConfigurationFactory()
            .create(environmentVariables, "zeebe.broker", configPath, BrokerCfg.class);
    config.init(BROKER_BASE, environmentVariables);

    return config;
  }
}
