(ns event-data-deposit-stasher.stash
  (:require [event-data-deposit-stasher.lagotto :as lagotto]
            [event-data-deposit-stasher.util :as util])
  (:require [clojure.tools.logging :as l]
            [clojure.java.io :as io]
            [clojure.data.json :as json])
  (:require [config.core :refer [env]])
  (:require [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clj-time.coerce :as clj-time-coerce]))

(def ymd (clj-time-format/formatter "yyyy-MM-dd"))



(defn save-deposits-json
  "Save a sequence of deposit objects in a temp file, return it."
  [deposits]
  (let [tempfile (java.io.File/createTempFile "event-data-deposit-stasher" nil)
       structure {"meta" {"status" "ok" "message-type" "deposit-list" "message-version" "7" "total" (count deposits) "total-pages" 1 "page" 1} "deposits" deposits}]
    (with-open [writer (io/writer tempfile)]
      (json/write structure writer))
  tempfile))

(defn run-all
  "Run the whole process of archiving, splitting and uploading a day's logs with start and end date string."
  [start-date end-date]
  (let [; All deposits as Clojure objects
        all-deposits (lagotto/fetch-all-deposits start-date end-date)]
    (l/info "Run all on" start-date "ending" end-date)
    (l/info "Got" (count all-deposits) "deposits")
    (let [source-ids (lagotto/source-ids all-deposits)
          all-file (save-deposits-json all-deposits)]
      (util/upload-file all-file (str (clj-time-format/unparse ymd start-date) "/all.json" ))
      (.delete all-file)
      (l/info "Working with following source-ids" source-ids)
      (doseq [source-id source-ids]
        (let [filtered (filter #(= (get % "source_id") source-id) all-deposits)
              filtered-file (save-deposits-json filtered)]
          (util/upload-file filtered-file (str (clj-time-format/unparse ymd start-date) "/" source-id ".json" ))
          (.delete filtered-file))))))

