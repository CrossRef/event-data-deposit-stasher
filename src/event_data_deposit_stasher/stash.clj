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



(defn run-all
  "Run the whole process of archiving, splitting and uploading a day's logs with start and end date string."
  [start-date end-date]
  (let [; All deposits as Clojure objects
        all-deposits (lagotto/fetch-all-deposits start-date end-date)
        normalized (map util/transform-deposit all-deposits)
        date-str (clj-time-format/unparse ymd start-date)

        ; Group into pathname => file
        all {(str "collected/" date-str "/events.json") normalized}
        source (group-and-project normalized #(str "collected/" date-str "/sources/" (:source_id %) "/events.json") identity)
        doi (group-and-project normalized #(str "collected/" date-str "/works/" (cr-doi/non-url-doi (:obj_id %)) "/events.json") identity)
        doi-source (group-and-project normalized #(str "collected/" date-str "/works/" (cr-doi/non-url-doi (:obj_id %)) "/sources/" (:source_id %) "/events.json") identity)

        ; all-results (merge all source doi doi-source)
        all-results (merge all source doi doi-source)]

        (doseq [[keyname data] all-results]
          (let [f (save-deposits-json data)]
            (util/upload-file f keyname "application/json")
            (.delete f)))))
