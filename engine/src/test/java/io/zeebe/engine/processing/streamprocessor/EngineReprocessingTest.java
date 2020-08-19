/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.streamprocessor;

import static io.zeebe.protocol.record.intent.WorkflowInstanceIntent.ELEMENT_ACTIVATED;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.stream.StreamWrapperException;
import java.util.stream.IntStream;
import org.assertj.core.internal.bytebuddy.utility.RandomString;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class EngineReprocessingTest {

  private static final String PROCESS_ID = "process";

  private static final BpmnModelInstance SIMPLE_FLOW =
      Bpmn.createExecutableProcess(PROCESS_ID).startEvent().endEvent().done();
  @Rule public EngineRule engineRule = EngineRule.singlePartition();

  @Before
  public void init() {
    engineRule.deployment().withXmlResource(SIMPLE_FLOW).deploy();
    IntStream.range(0, 100)
        .forEach(
            i ->
                engineRule
                    .workflowInstance()
                    .ofBpmnProcessId(PROCESS_ID)
                    .withVariable("data", RandomString.make(1024))
                    .create());

    Awaitility.await()
        .until(
            () ->
                RecordingExporter.workflowInstanceRecords()
                    .withElementType(BpmnElementType.PROCESS)
                    .withIntent(ELEMENT_ACTIVATED)
                    .limit(100)
                    .count(),
            (count) -> count == 100);

    engineRule.stop();
  }

  @Test
  public void shouldReprocess() {
    // given - reprocess
    final int lastSize = RecordingExporter.getRecords().size();
    // we need to reset the record exporter
    RecordingExporter.reset();
    engineRule.start();

    // when - then
    Awaitility.await("Await reprocessing of " + lastSize)
        .until(() -> RecordingExporter.getRecords().size(), (size) -> size >= lastSize);
  }

  @Test
  public void shouldContinueProcessingAfterReprocessing() {
    // given - reprocess
    final int lastSize = RecordingExporter.getRecords().size();
    // we need to reset the record exporter
    RecordingExporter.reset();
    engineRule.start();

    Awaitility.await("Await reprocessing of " + lastSize)
        .until(() -> RecordingExporter.getRecords().size(), (size) -> size >= lastSize);

    // when - then
    engineRule.workflowInstance().ofBpmnProcessId(PROCESS_ID).create();
  }

  @Test
  public void shouldNotContinueProcessingWhenPausedDuringReprocessing() {
    // given - reprocess
    final int lastSize = RecordingExporter.getRecords().size();
    // we need to reset the record exporter
    RecordingExporter.reset();
    engineRule.start();
    engineRule.pauseProcessing(1);

    // when
    Awaitility.await("Await reprocessing of " + lastSize)
        .until(() -> RecordingExporter.getRecords().size(), (size) -> size >= lastSize);

    // then
    Assert.assertThrows(
        StreamWrapperException.class,
        () -> engineRule.workflowInstance().ofBpmnProcessId(PROCESS_ID).create());
  }

  @Test
  public void shouldContinueAfterReprocessWhenProcessingWasResumed() {
    // given
    final int lastSize = RecordingExporter.getRecords().size();
    // we need to reset the record exporter
    RecordingExporter.reset();
    engineRule.start();
    engineRule.pauseProcessing(1);
    engineRule.resumeProcessing(1);

    Awaitility.await("Await reprocessing of " + lastSize)
        .until(() -> RecordingExporter.getRecords().size(), (size) -> size >= lastSize);

    // when
    engineRule.workflowInstance().ofBpmnProcessId(PROCESS_ID).create();
  }
}
