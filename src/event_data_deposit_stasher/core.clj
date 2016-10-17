(ns event-data-deposit-stasher.core
  (:require [event-data-deposit-stasher.lagotto :as lagotto]
            [event-data-deposit-stasher.stash :as stash]
            [event-data-deposit-stasher.util :as util])
  (:require [clojure.set :as set]
            [clojure.tools.logging :as l])
  (:require [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clj-time.coerce :as clj-time-coerce])
  (:require [config.core :refer [env]])
  (:gen-class))

(def config-keys
  #{
    :archive-s3-bucket ; S3 Bucket name that holds archived deposits.
    :query-s3-bucket ; S3 Bucket name that holds query API data.
    :s3-access-key-id ; AWS access key for putting logs.
    :s3-secret-access-key ; AWS access key for putting logs.
    :lagotto-instance-url ; full base URL of lagotto instance.
  })

; also :exclude-sources : comma-separated value of source names to exclude in query API.

(defn missing-config-keys
  "Ensure all config keys are present. Return missing keys or nil if OK."
  []
  (let [missing (set/difference config-keys (set (keys env)))]
    (when-not (empty? missing)
      missing)))

(defn day-to-day-interval
  "For a given day date, give the start and end dates for it"
  [day]
  (let [day-before (clj-time/minus day (clj-time/days 1))]
        [(clj-time/date-midnight (clj-time/year day-before) (clj-time/month day-before) (clj-time/day day-before))
         (clj-time/date-midnight (clj-time/year day) (clj-time/month day) (clj-time/day day))]))

(defn get-excluded-sources
  "Get set of sources to exclude. Should be nil set except for unforseen circumstances."
  []
  (set (.split (or (:exclude-sources env) "") ",")))

(def ymd (clj-time-format/formatter "yyyy-MM-dd"))

(defn archive
  "Daily archive task. Retrieve deposits and upload to Archive bucket."
  [num-back-days]
  (l/info "Daily Archive Task")
  ; Stash for the last n days if the job's not been done.
  (let [now (clj-time/now)
        interval-range (map #(day-to-day-interval (clj-time/minus now (clj-time/days %))) (range 0 num-back-days))]
    (l/info "Checking " (count interval-range) "past days")
    (doseq [[interval-start interval-end] interval-range]
      (let [start-str (clj-time-format/unparse ymd interval-start)
            end-str (clj-time-format/unparse ymd interval-end)]
        (l/info "Check " start-str "to" end-str)
          (let [expected-log-name (str "collected/" start-str "/deposits.json")]
            (l/info "Check" expected-log-name "exists")
            (when-not (.doesObjectExist @util/aws-client (:archive-s3-bucket env) expected-log-name)
              (l/info "Doesn't exist, fetch and create.")
              (stash/archive interval-start interval-end expected-log-name)))))))



(defn update-query-api
  "Daily query processing task. Retrieve latest deposits from the Archive bucket, slice and upload to Query bucket."
  [num-back-days]
  (l/info "Daily Archive Task")
  ; Stash for the last n days if the job's not been done.
  (let [interval-range (map #(day-to-day-interval (clj-time/minus (clj-time/now) (clj-time/days %))) (range 0 num-back-days))]
    (l/info "Checking " (count interval-range) "past days")
    (doseq [[interval-start _] interval-range]
      (let [start-str (clj-time-format/unparse ymd interval-start)]
        (l/info "Check " start-str)
          (let [; Input (deposits) and output (events) filenames. These live in different buckets.
                input-name (str "collected/" start-str "/deposits.json")

                ; Markers for whether or not this day's worth of collected events have been saved in 'collected' and 'occurred' respectively.

                ; Test output name that's generated along with a load of other queries. 
                collected-flag-name (str "__CONTROL__/collected/" start-str)

                ; Test output name that's generated along with a load of other queries. 
                occurred-flag-name (str "__CONTROL__/occurred/" start-str)

                input-exists (.doesObjectExist @util/aws-client (:archive-s3-bucket env) input-name)
                collected-flag-exists (.doesObjectExist @util/aws-client (:query-s3-bucket env) collected-flag-name)
                occurred-flag-exists (.doesObjectExist @util/aws-client (:query-s3-bucket env) occurred-flag-name)]
            
            (l/info "Check input" input-name "exists:" input-exists)
            (l/info "Check output flag 'collected' " collected-flag-name "exists: " collected-flag-exists)
            (l/info "Check output flag 'occurred' " occurred-flag-name "exists: " occurred-flag-exists)

            ; Ability to re-create either independently depending on whether the flag exists (or has been removed to re-process).
            (when (and input-exists (or
                                      (not collected-flag-exists)
                                      (not occurred-flag-exists)))
              (let [input (util/download-json-file (:archive-s3-bucket env) input-name)
                    deposits (get input "deposits")
                    ; Normalize to Event Data Schema.
                    normalized (map util/transform-deposit deposits)
                    ; Exclude sources if necessary.
                    excluded-sources (get-excluded-sources)
                    filtered-source (remove #(excluded-sources (% "source_id")) normalized)]

                (when (not collected-flag-exists)
                  (l/info "Update query API 'collected'" start-str)
                  (stash/update-query-api-collected filtered-source start-str)
                  (util/upload-bytes (byte-array 0) (:query-s3-bucket env) collected-flag-name "application/json"))

                (when (not occurred-flag-exists)
                  (l/info "Update query API 'occurred'" start-str)
                  (stash/update-query-api-occurred filtered-source start-str)
                  (util/upload-bytes (byte-array 0) (:query-s3-bucket env) occurred-flag-name "application/json")))))))))

(defn invalid-command
  [command]
  (l/fatal "Invalid command: " command))


(defn -main
  [& args]
  (l/info "Starting Event Data Deposit Log Stasher")

  (when-let [missing (missing-config-keys)]
    (l/fatal "Missing keys" missing)
    (System/exit 1))

  (l/info "Starting Event Data Deposit Log Stasher")

  (condp = (first args)
    ; To run every day and generate data.
    "daily-archive" (archive 1)
    "daily-load-query-api" (update-query-api 1)

    ; To generate back-data.
    ; Data collected before May 2016 isn't consistent enough.
    "historical-archive" (archive 100)
    "historical-load-query-api" (update-query-api 100)
    (invalid-command (first args))))
