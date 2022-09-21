(ns sparkfacts.massager
  (:require [meander.epsilon :as m]
            [xtdb.api :as xt]))


 (defn get-inst
    [ms]
   (java.util.Date. ms))

(defn xt-app-id
  [cid]
  (format "cid:%s" cid))

(defn xt-job-id
  [cid job-id]
  (format "cid:%s;job:%s" cid job-id))

(defn xt-stage-ref-id
  [cid stage-id]
  (format "cid:%s;stage:%s" cid stage-id))

(defn xt-stage-attempt-id
  [cid stage-id stage-attempt]
  (format "cid:%s;stage:%s;sattempt:%s" cid stage-id stage-attempt))

(defn xt-task-ref-id
  [cid stage-id stage-attempt task-id]
  (format "cid:%s;stage:%s;sattempt:%s;task:%s" cid stage-id stage-attempt task-id))

(defn xt-task-attempt-id
  [cid stage-id stage-attempt task-id task-attempt]
  (format "cid:%s;stage:%s;sattempt:%s;task:%s;tattempt:%s"
          cid
          stage-id
          stage-attempt
          task-id
          task-attempt))

(defmulti massager (fn [_ e]
                     (:event e)))

(defmethod massager "SparkListenerApplicationStart"
  [cid e]
  (m/match e
    {:event ?event,
     :app_name ?name,
     :app_id ?id,
     :timestamp ?ts}

    (let [inst (get-inst ?ts)]
      [[::xt/put
        {:xt/id (xt-app-id cid)
         :app/name ?name
         :app/id ?id
         :app/submission-time inst}
        inst]])))


(defmethod massager "SparkListenerApplicationEnd"
  [cid e]
  (m/match e
    {:event ?event,
     :timestamp ?ts}

    (let [inst (get-inst ?ts)]
      [[::xt/fn "merge" (xt-app-id cid)
        {:app/completion-time inst}
        [inst]]])))


(defmethod massager "SparkListenerJobStart"
  [cid e]
  (m/match e
    {:event ?event
     :job_id ?id
     :submission_time ?ts
     :stage_ids ?stage-ids}

    (let[inst (get-inst ?ts)
         placeholder-stages (mapv (partial xt-stage-ref-id cid) ?stage-ids)]

      (into
       [[::xt/fn "conj-k" (xt-app-id cid)
         :app/jobs (xt-job-id cid ?id)
         [inst]]

        [::xt/put
         {:xt/id (xt-job-id cid ?id)
          :job/id ?id
          :job/submission-time inst
          :job/stages placeholder-stages}
         inst]]

       (mapv (fn [placeholder-stage-id]
               [::xt/put
                {:xt/id placeholder-stage-id
                 :stage/attempt-refs []}
                inst])
             placeholder-stages)))))


(defmethod massager "SparkListenerJobEnd"
  [cid e]
  (m/match e
    {:event ?event
     :job_id ?id
     :completion_time ?ts
     :job_result {:result ?result}}

    (let [inst (get-inst ?ts)]
      [[::xt/fn "merge" (xt-job-id cid ?id)
        {:job/completion-time inst
         :job/result ?result}
        [inst]]])))


(defmethod massager "SparkListenerStageSubmitted"
  [cid e]
  (m/match e
    {:event ?e
     :stage_info
     {:number_of_tasks ?task-count
      :stage_id ?stage-id
      :stage_name ?stage-name
      :stage_attempt_id ?stage-attempt-id
      :parent_ids ?parent-ids
      :submission_time ?ts}}

    (let [inst (get-inst ?ts)]
      ;; create a stage attempt doc
      [[::xt/put
        {:xt/id (xt-stage-attempt-id cid ?stage-id ?stage-attempt-id)
         :stage/id ?stage-id
         :stage/attempt ?stage-attempt-id
         :stage/name ?stage-name
         :stage/parent-ids (mapv (partial xt-stage-ref-id cid) ?parent-ids)
         :stage/task-count ?task-count
         :stage/tasks []
         :stage/submission-time inst}
        inst]

       ;; add the attempt doc into placeholder stage attempt-refs
       [::xt/fn "conj-k" (xt-stage-ref-id cid ?stage-id)
        :stage/attempt-refs (xt-stage-attempt-id cid ?stage-id ?stage-attempt-id)
        [inst]]])))


(defmethod massager "SparkListenerStageCompleted"
  [cid e]
  (m/match e
    {:event ?e
     :stage_info
     {:stage_id ?stage-id
      :stage_attempt_id ?stage-attempt-id
      :completion_time ?completion-time
      :failure_reason ?reason}}

    (let [inst (get-inst ?completion-time)]
      [[::xt/fn "merge" (xt-stage-attempt-id cid ?stage-id ?stage-attempt-id)
        {:stage/completion-time inst
         :stage/reason (if ?reason ?reason "succeeded")}
        [inst]]])))


(defmethod massager "SparkListenerTaskStart"
  [cid e]
  (m/match e
    {:event ?e
     :stage_id ?stage-id
     :stage_attempt_id ?stage-attempt-id
     :task_info
     {:task_id ?task-id
      :index ?index
      :attempt ?task-attempt-id
      :launch_time ?launch-time
      :executor_id ?executor-id
      :host ?host
      :locality ?locality
      :finish_time ?finish-time
      :failed ?failed
      :killed ?killed}}

    (let [inst (get-inst ?launch-time)
          create-task-placeholder [[::xt/put
                                    {:xt/id (xt-task-ref-id cid ?stage-id ?stage-attempt-id ?task-id)
                                     :task/attempt-refs []}
                                    inst]]]

      (vec (concat
            (when (= 0 ?task-attempt-id)
              create-task-placeholder)

            [[::xt/fn "conj-k" (xt-stage-attempt-id cid ?stage-id ?stage-attempt-id)
              :stage/tasks (xt-task-ref-id cid ?stage-id ?stage-attempt-id ?task-id)
              [inst]]

             [::xt/put
              {:xt/id (xt-task-attempt-id cid ?stage-id ?stage-attempt-id ?task-id ?task-attempt-id)
               :task/id ?task-id
               :task/index ?index
               :task/attempt ?task-attempt-id
               :task/host ?host
               :task/executor-id ?executor-id
               :task/locality ?locality
               :task/submission-time ?launch-time
               :task/failed ?failed
               :task/killed ?killed}
              inst]

             [::xt/fn "conj-k" (xt-task-ref-id cid ?stage-id ?stage-attempt-id ?task-id)
              :task/attempt-refs (xt-task-attempt-id cid ?stage-id ?stage-attempt-id ?task-id ?task-attempt-id)
              [inst]]])))))


(defmethod massager "SparkListenerTaskEnd"
  [cid e]
  (m/match e
    {:event ?e
     :stage_id ?stage-id
     :stage_attempt_id ?stage-attempt-id
     :task_type ?task-type
     :task_end_reason {:reason ?reason}
     :task_info
     {:task_id ?task-id
      :index ?index
      :attempt ?task-attempt-id
      :finish_time ?finish-time
      :failed ?failed
      :killed ?killed}
     :task_metrics
     {:executor_run_time ?executor-run-time
      :executor_cpu_time ?executor-cpu-time}}

    (let [inst (get-inst ?finish-time)]
      [[::xt/fn "merge" (xt-task-attempt-id cid ?stage-id ?stage-attempt-id ?task-id ?task-attempt-id)
        {:task/type ?task-type
         :task/reason ?reason
         :task/completion-time inst
         :task/executor-run-time ?executor-run-time
         :task/executor-cpu-time ?executor-cpu-time
         :task/failed ?failed
         :task/killed ?killed}
        [inst]]])))


#_(defmethod massager "SparkListenerExecutorAdded"
  [e]
  (m/match e
    {:event ?event
     :timestamp ?ts
     :executor_id ?executor-id
     :executor_info
     {:host ?ip
      :total_cores ?cores
      :log_urls {}
      :attributes {}
      :resources {}
      :resource_profile_id 0}}

    {:event ?event
     :timestamp ?ts
     :executor/id ?executor-id
     :executor/host ?ip
     :executor/cores ?cores}))
#_#_#_#_#_#_#_


(defmethod massager "SparkListenerExecutorRemoved"
  [e]
  (m/match e
    {:event ?event
     :timestamp ?ts
     :executor_id ?executor-id
     :removed_reason ?reason}

    {:event ?event
     :timestamp ?ts
     :executor/id ?executor-id
     :executor/removal-reason ?reason}))


(defmethod massager "org.apache.spark.scheduler.SparkListenerExecutorExcluded"
  [e]
  (m/match e
    {:event ?event
     :time ?ts
     :executorid ?executor-id
     :taskfailures ?count}

    {:event ?event
     :timestamp ?ts
     :executor/id ?executor-id
     :executor/task-failure-count ?count}))


(defmethod massager "org.apache.spark.scheduler.SparkListenerExecutorExcludedForStage"
  [e]
  (m/match e
    {:event ?event
     :time ?ts
     :executorid ?executor-id
     :stageid ?stage-id
     :stageattemptid ?stage-attempt-id
     :taskfailures ?count}

    {:event ?event
     :timestamp ?ts
     :executor/id ?executor-id
     :executor/task-failure-count ?count
     :executor/excluded-for-stage-id ?stage-id
     :executor/excluded-for-stage-attempt ?stage-attempt-id}))


(defmethod massager "org.apache.spark.scheduler.SparkListenerExecutorUnexcluded"
  [e]
  (m/match e
    {:event ?event
     :time ?ts
     :executorid ?executor-id}

    {:event ?event
     :timestamp ?ts
     :executor/id ?executor-id}))


(defmethod massager "org.apache.spark.scheduler.SparkListenerNodeExcluded"
  [e]
  (m/match e
    {:event ?event
     :time ?ts
     :hostid ?ip
     :executorfailures ?count}

    {:event ?event
     :timestamp ?ts
     :host/ip ?ip
     :host/task-failure-count ?count}))


(defmethod massager "org.apache.spark.scheduler.SparkListenerNodeExcludedForStage"
  [e]
  (m/match e
    {:event ?event
     :time ?ts
     :hostid ?ip
     :stageid ?stage-id
     :stageattemptid ?stage-attempt-id
     :executorfailures ?count}

    {:event ?event
     :timestamp ?ts
     :host/ip ?ip
     :host/task-failure-count ?count
     :host/excluded-for-stage-id ?stage-id
     :host/excluded-for-stage-attempt ?stage-attempt-id}))


(defmethod massager "org.apache.spark.scheduler.SparkListenerNodeUnexcluded"
  [e]
  (m/match e
    {:event ?event
     :time ?ts
     :hostid ?ip}

    {:event ?event
     :timestamp ?ts
     :host/ip ?ip}))


(defmethod massager :default
  [_ _]
  {:undefined true})
