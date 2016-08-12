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
    :archive-s3-bucket ; S3 Bucket name that holds archive information.
    :s3-access-key-id ; AWS access key for putting logs.
    :s3-secret-access-key ; AWS access key for putting logs.
    :lagotto-instance-url ; full base URL of lagotto instance.
  })

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

(def ymd (clj-time-format/formatter "yyyy-MM-dd"))

; Number of days' worth of daily tasks. If this is run every night, it will only do yesterday's.
; But can miss up to this many days.
(def num-back-days 100)

(defn daily
  "Daily task. Retrieve deposits, slice, upload."
  []
  (l/info "Daily Task")
  ; Stash for the last n days if the job's not been done.
  (let [interval-range (map #(day-to-day-interval (clj-time/minus (clj-time/now) (clj-time/days %))) (range 0 num-back-days))]
    (l/info "Checking " (count interval-range) "past days")
    (doseq [[interval-start interval-end] interval-range]
      (let [start-str (clj-time-format/unparse ymd interval-start)
            end-str (clj-time-format/unparse ymd interval-end)]
        (l/info "Check " start-str "to" end-str)
          (let [expected-log-name (str "collected/" start-str "/events.json")]
            (l/info "Check" expected-log-name "exists")
            (when-not (.doesObjectExist @util/aws-client (:archive-s3-bucket env) expected-log-name)
              (l/info "Doesn't exist, fetch and create.")
              (stash/run-all interval-start interval-end)))))))

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
    "daily" (daily)
    (invalid-command (first args))))
