(ns event-data-deposit-stasher.lagotto
  (:require [config.core :refer [env]])
  (:require [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clj-time.coerce :as clj-time-coerce])
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as l])
  (:require [org.httpkit.client :as http-client]))

(def ymd (clj-time-format/formatter "yyyy-MM-dd"))

(defn get-all-pages
  [start-date-str end-date-str url]
  (loop [page-number 1
         deposits (list)]
    (l/info "Get" url "from" start-date-str "until" end-date-str "page" page-number)
    (let [response @(http-client/get url {:query-params {"page" page-number "from_date" start-date-str "until_date" end-date-str}})
          parsed (json/read-str (:body response))
          page-deposits (get-in parsed ["deposits"])]

      ; keep doing until we get an empty page.
      (if (empty? page-deposits)
        deposits
        (recur (inc page-number) (concat deposits page-deposits))))))

(defn fetch-all-deposits
  [start-date end-date]
  (let [url (str (:lagotto-instance-url env) "/api/deposits")
        start-date (clj-time-format/unparse ymd start-date)
        end-date (clj-time-format/unparse ymd end-date)
        data (get-all-pages start-date end-date url)]
    data))

(defn source-ids
  [deposits]
  (distinct (map #(get % "source_id") deposits)))
