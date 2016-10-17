(ns event-data-deposit-stasher.stash
  (:require [event-data-deposit-stasher.lagotto :as lagotto]
            [event-data-deposit-stasher.util :as util])
  (:require [clojure.tools.logging :as l]
            [clojure.java.io :as io]
            [clojure.data.json :as json])
  (:require [config.core :refer [env]])
  (:require [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clj-time.coerce :as clj-time-coerce]
            [crossref.util.doi :as cr-doi]))

(def ymd (clj-time-format/formatter "yyyy-MM-dd"))

(def num-threads 50)

(defn save-deposits-json
  "Save a sequence of deposit objects in a temp file, return it."
  [deposits]
  (let [tempfile (java.io.File/createTempFile "event-data-deposit-stasher" nil)
       structure {"meta" {"status" "ok" "message-type" "deposit-list" "message-version" "7" "total" (count deposits) "total-pages" 1 "page" 1} "deposits" deposits}]
    (with-open [writer (io/writer tempfile)]
      (json/write structure writer))
  tempfile))

(defn group-and-project
  "Group a seq by the group-function, values projected by project-f"
  [coll group-f project-f]
  (loop [grouped (transient {})
         coll coll]
    (if (empty? coll)
      (persistent! grouped)
      (let [h (first coll)
            t (rest coll)
            p (project-f h)
            g (group-f h)
            grouped (assoc! grouped g (conj (get grouped g) p))]
        (recur grouped t)))))


(defn archive
  "Archive Deposit data for given day."
  [start-date end-date key-name]
  (let [; All deposits as Clojure objects
        all-deposits (lagotto/fetch-all-deposits start-date end-date)
        f (save-deposits-json all-deposits)]
      (util/upload-file f (:archive-s3-bucket env) key-name "application/json")
      (.delete f)))


(defn upload-in-thread
  "Start an upload of a chunk of JSON objects in a Thread. Return the Thread immediately.
  Items is a seq of [key, data] tuples."
  [items]
  (let [num-items (count items)
        thread (new Thread (fn []
                  (loop [[[[keyname prev-url next-url] data] & rest-items] items
                         c 0]

                    (when (zero? (mod c 10))
                      (l/info "Uploaded" c "/" num-items "," (count rest-items) "remaining on" (.toString (Thread/currentThread))))

                    (let [response-data {"message-type" "event-list" "total-events" (count data) "events" data "previous" prev-url "next" next-url}
                          ^java.io.ByteArrayOutputStream buffer (new java.io.ByteArrayOutputStream)
                          stream (new java.io.OutputStreamWriter buffer)]
                      (json/write response-data stream)
                      (.close stream)
                      (util/upload-bytes (.toByteArray buffer) (:query-s3-bucket env) keyname "application/json"))
                    (if (empty? rest-items)
                      (l/info "FINISHED" c "/" num-items "on" (.toString (Thread/currentThread)))
                      (recur rest-items (inc c))))))]
  (l/info "Create thread" thread)
  (.start thread)
  (l/info "Return" thread)
  thread))

(defn merge-upload-in-thread
  "Start an upload/merge of a chunk of JSON objects in a Thread. Return the Thread immediately.
  Items is a seq of [key, data] tuples."
  [items]
  (let [num-items (count items)
        thread (new Thread (fn []
                  (loop [[[[keyname prev-url next-url] data] & rest-items] items
                         c 0]

                    (when (zero? (mod c 10))
                      (l/info "Uploaded" c "/" num-items "," (count rest-items) "remaining on" (.toString (Thread/currentThread))))

                    (let [previous (util/download-json-file (:query-s3-bucket env) keyname)
                          previous-data (get previous "items")
                          merged-data (concat data previous-data)
                          response-data {"message-type" "event-list" "total-events" (count merged-data) "events" data "previous" prev-url "next" next-url}
                          ^java.io.ByteArrayOutputStream buffer (new java.io.ByteArrayOutputStream)
                          stream (new java.io.OutputStreamWriter buffer)]
                      (json/write response-data stream)
                      (.close stream)
                      (util/upload-bytes (.toByteArray buffer) (:query-s3-bucket env) keyname "application/json")
                      (l/info "Merge prev" (count previous-data) "with" (count data) "into" (count merged-data)))

                    (if (empty? rest-items)
                      (l/info "FINISHED" c "/" num-items "on" (.toString (Thread/currentThread)))
                      (recur rest-items (inc c))))))]
  (l/info "Create thread" thread)
  (.start thread)
  (l/info "Return" thread)
  thread))


(defn date-str-from-occurred [event]
  (clj-time-format/unparse ymd (clj-time-format/parse (get event :occurred_at))))

(defn prev-date-str [date-str]
  (clj-time-format/unparse ymd (clj-time/minus (clj-time-format/parse ymd date-str) (clj-time/days 1))))

(defn next-date-str [date-str]
  (clj-time-format/unparse ymd (clj-time/plus (clj-time-format/parse ymd date-str) (clj-time/days 1))))


(defn update-query-api-collected
  "Load Query API with data collected on given day."
  [deposits date-str]
  (let [url-base (:url-base env)

        ; Group into pathname => file

        all {[(str "collected/" date-str "/events.json")
              (str url-base "collected/" (prev-date-str date-str) "/events.json")
              (str url-base "collected/" (next-date-str date-str) "/events.json")]
              deposits}

        source (group-and-project deposits (fn [event]
                                          (let [date-str (date-str-from-occurred event)]
                                            [(str "collected/" date-str "/sources/" (:source_id event) "/events.json")
                                             (str url-base "collected/" (prev-date-str date-str) "/sources/" (:source_id event) "/events.json")
                                             (str url-base "collected/" (next-date-str date-str) "/sources/" (:source_id event) "/events.json")])) identity)
        
        doi (group-and-project deposits (fn [event]
                                          (let [date-str (date-str-from-occurred event)]
                                            [(str "collected/" date-str "/works/" (cr-doi/non-url-doi (:obj_id event)) "/events.json")
                                             (str url-base "collected/" (prev-date-str date-str) "/works/" (cr-doi/non-url-doi (:obj_id event)) "/events.json")
                                             (str url-base "collected/" (next-date-str date-str) "/works/" (cr-doi/non-url-doi (:obj_id event)) "/events.json")])) identity)
        
        doi-source (group-and-project deposits (fn [event]
                                          (let [date-str (date-str-from-occurred event)]
                                            [(str "collected/" date-str "/works/" (cr-doi/non-url-doi (:obj_id event)) "/sources/" (:source_id event) "/events.json")
                                             (str url-base "collected/" (prev-date-str date-str) "/works/" (cr-doi/non-url-doi (:obj_id event)) "/sources/" (:source_id event) "/events.json")
                                             (str url-base "collected/" (next-date-str date-str) "/works/" (cr-doi/non-url-doi (:obj_id event)) "/sources/" (:source_id event) "/events.json")])) identity)

        all-results (merge all source doi doi-source)
        num-queries (count all-results)
        partition-size (/ num-queries num-threads)

        partitions (partition-all partition-size all-results)
        running-partitions (doall (map upload-in-thread partitions))]

  (l/info "Got" (count deposits) "deposits")
  (l/info "Resulting in" num-queries "queries")
  (l/info "Uploading in" (count partitions) "partitions...")

  (doseq [p running-partitions]
    (.join p))

  (l/info "Done!")))


(defn update-query-api-occurred
  "Load Query API with data that occurred on a given day."
  [deposits date-str]
  (let [url-base (:url-base env)

        ; We have a load of events. Project each event into the key it will have in the bucket. Also supply the previous and next urls, which will need to be carried through.

        ; Group into pathname => file
        all (group-and-project deposits (fn [event]
                                          (let [date-str (date-str-from-occurred event)]
                                            [(str "occurred/" date-str "/events.json")
                                             (str url-base "occurred/" (prev-date-str date-str) "/events.json")
                                             (str url-base "occurred/" (next-date-str date-str) "/events.json")])) identity)

        source (group-and-project deposits (fn [event]
                                          (let [date-str (date-str-from-occurred event)]
                                            [(str "occurred/" date-str "/sources/" (:source_id event) "/events.json")
                                             (str url-base "occurred/" (prev-date-str date-str) "/sources/" (:source_id event) "/events.json")
                                             (str url-base "occurred/" (next-date-str date-str) "/sources/" (:source_id event) "/events.json")])) identity)
        
        doi (group-and-project deposits (fn [event]
                                          (let [date-str (date-str-from-occurred event)]
                                            [(str "occurred/" date-str "/works/" (cr-doi/non-url-doi (:obj_id event)) "/events.json")
                                             (str url-base "occurred/" (prev-date-str date-str) "/works/" (cr-doi/non-url-doi (:obj_id event)) "/events.json")
                                             (str url-base "occurred/" (next-date-str date-str) "/works/" (cr-doi/non-url-doi (:obj_id event)) "/events.json")])) identity)
        
        doi-source (group-and-project deposits (fn [event]
                                          (let [date-str (date-str-from-occurred event)]
                                            [(str "occurred/" date-str "/works/" (cr-doi/non-url-doi (:obj_id event)) "/sources/" (:source_id event) "/events.json")
                                             (str url-base "occurred/" (prev-date-str date-str) "/works/" (cr-doi/non-url-doi (:obj_id event)) "/sources/" (:source_id event) "/events.json")
                                             (str url-base "occurred/" (next-date-str date-str) "/works/" (cr-doi/non-url-doi (:obj_id event)) "/sources/" (:source_id event) "/events.json")])) identity)

        all-results (merge all source doi doi-source)
        num-queries (count all-results)
        partition-size (/ num-queries num-threads)

        partitions (partition-all partition-size all-results)
        running-partitions (doall (map merge-upload-in-thread partitions))]

  (l/info "Got" (count deposits) "deposits")
  (l/info "Resulting in" num-queries "queries")
  (l/info "Uploading in" (count partitions) "partitions...")

  (doseq [p running-partitions]
    (.join p))

  (l/info "Done!")))



