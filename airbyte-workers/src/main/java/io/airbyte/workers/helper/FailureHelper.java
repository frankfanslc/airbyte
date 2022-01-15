/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureSource;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.Metadata;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class FailureHelper {

  private static final String JOB_ID_METADATA_KEY = "jobId";
  private static final String ATTEMPT_METADATA_KEY = "attempt";

  private static final String WORKFLOW_TYPE_SYNC = "SyncWorkflow";
  private static final String ACTIVITY_TYPE_REPLICATE = "Replicate";
  private static final String ACTIVITY_TYPE_PERSIST = "Persist";
  private static final String ACTIVITY_TYPE_NORMALIZE = "Normalize";
  private static final String ACTIVITY_TYPE_DBT_RUN = "Run";

  public static FailureReason genericFailure(final Throwable t, final Long jobId, final Integer attempt) {
    return new FailureReason()
        .withInternalMessage(t.getMessage())
        .withStacktrace(ExceptionUtils.getStackTrace(t))
        .withTimestamp(System.currentTimeMillis())
        .withMetadata(new Metadata()
            .withAdditionalProperty(JOB_ID_METADATA_KEY, jobId)
            .withAdditionalProperty(ATTEMPT_METADATA_KEY, attempt));
  }

  public static FailureReason sourceFailure(final Throwable t, final Long jobId, final Integer attempt) {
    return genericFailure(t, jobId, attempt)
        .withFailureSource(FailureSource.SOURCE)
        .withFailureType(FailureType.UNKNOWN)
        .withExternalMessage("Something went wrong within the source connector");
  }

  public static FailureReason destinationFailure(final Throwable t, final Long jobId, final Integer attempt) {
    return genericFailure(t, jobId, attempt)
        .withFailureSource(FailureSource.DESTINATION)
        .withFailureType(FailureType.UNKNOWN)
        .withExternalMessage("Something went wrong within the destination connector");
  }

  public static FailureReason replicationWorkerFailure(final Throwable t, final Long jobId, final Integer attempt) {
    return genericFailure(t, jobId, attempt)
        .withFailureSource(FailureSource.REPLICATION_WORKER)
        .withFailureType(FailureType.UNKNOWN)
        .withExternalMessage("Something went wrong during replication");
  }

  public static FailureReason persistenceFailure(final Throwable t, final Long jobId, final Integer attempt) {
    return genericFailure(t, jobId, attempt)
        .withFailureSource(FailureSource.PERSISTENCE)
        .withFailureType(FailureType.UNKNOWN)
        .withExternalMessage("Something went wrong during state persistence");
  }

  public static FailureReason normalizationFailure(final Throwable t, final Long jobId, final Integer attempt) {
    return genericFailure(t, jobId, attempt)
        .withFailureSource(FailureSource.NORMALIZATION)
        .withFailureType(FailureType.UNKNOWN)
        .withExternalMessage("Something went wrong during normalization");
  }

  public static FailureReason dbtFailure(final Throwable t, final Long jobId, final Integer attempt) {
    return genericFailure(t, jobId, attempt)
        .withFailureSource(FailureSource.DBT)
        .withFailureType(FailureType.UNKNOWN)
        .withExternalMessage("Something went wrong during dbt");
  }

  public static FailureReason unknownSourceFailure(final Throwable t, final Long jobId, final Integer attempt) {
    return genericFailure(t, jobId, attempt)
        .withFailureSource(FailureSource.UNKNOWN)
        .withFailureType(FailureType.UNKNOWN)
        .withExternalMessage("An unknown failure occurred");
  }

  public static AttemptFailureSummary failureSummary(final Set<FailureReason> failures, final Boolean partialSuccess) {
    return new AttemptFailureSummary()
        .withFailures(orderedFailures(failures))
        .withPartialSuccess(partialSuccess);
  }

  public static AttemptFailureSummary failureSummaryForCancellation(final Set<FailureReason> failures,
                                                                    final Boolean partialSuccess,
                                                                    final String cancelledBy) {
    return failureSummary(failures, partialSuccess)
        .withCancelled(true)
        .withCancelledBy(cancelledBy);
  }

  public static FailureReason failureReasonFromWorkflowAndActivity(final String workflowType,
                                                                   final String activityType,
                                                                   final Throwable t,
                                                                   final Long jobId,
                                                                   final Integer attempt) {
    if (workflowType.equals(WORKFLOW_TYPE_SYNC) && activityType.equals(ACTIVITY_TYPE_REPLICATE)) {
      return replicationWorkerFailure(t, jobId, attempt);
    } else if (workflowType.equals(WORKFLOW_TYPE_SYNC) && activityType.equals(ACTIVITY_TYPE_PERSIST)) {
      return persistenceFailure(t, jobId, attempt);
    } else if (workflowType.equals(WORKFLOW_TYPE_SYNC) && activityType.equals(ACTIVITY_TYPE_NORMALIZE)) {
      return normalizationFailure(t, jobId, attempt);
    } else if (workflowType.equals(WORKFLOW_TYPE_SYNC) && activityType.equals(ACTIVITY_TYPE_DBT_RUN)) {
      return dbtFailure(t, jobId, attempt);
    } else {
      return unknownSourceFailure(t, jobId, attempt);
    }
  }

  /**
   * Orders failures by timestamp, so that earlier failures come first in the list.
   */
  private static List<FailureReason> orderedFailures(final Set<FailureReason> failures) {
    return failures.stream().sorted(Comparator.comparing(FailureReason::getTimestamp)).collect(Collectors.toList());
  }

}
