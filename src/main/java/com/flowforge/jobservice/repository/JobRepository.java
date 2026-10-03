package com.flowforge.jobservice.repository;

import com.flowforge.jobservice.model.Job;
import com.flowforge.jobservice.model.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {
    @Modifying
    @Transactional
    @Query("""
            UPDATE Job j
            SET j.status = :newStatus,
                j.updatedAt = updatedAt
            WHERE j.id = :jobId
              AND j.status = :currentStatus
            """)
    int updateStatusIfCurrentStatus(
            @Param("jobId") UUID jobId,
            @Param("currentStatus") JobStatus currentStatus,
            @Param("newStatus") JobStatus newStatus,
            @Param("updatedAt") Instant updatedAt
    );
}
