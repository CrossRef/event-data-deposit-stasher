(ns event-data-deposit-stasher.util
  "Utility functions"
  (:require [config.core :refer [env]])
  (:require [clojure.tools.logging :as l])
  (:import [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
           [com.amazonaws.auth BasicAWSCredentials]
           [com.amazonaws.services.s3.model GetObjectRequest PutObjectRequest ObjectMetadata])
  (:import [redis.clients.jedis Jedis])
  (:import [java.util UUID])
  (:require [robert.bruce :refer [try-try-again]])
  (:gen-class))


(defn jedis-connection
  []
  (let [^Jedis redis-conn (new Jedis (:redis-host env) (Integer/parseInt (:redis-port env)))]
    (.select redis-conn (Integer/parseInt (:redis-db-number env)))
    redis-conn))

(defn aws-client
  []
  (new AmazonS3Client (new BasicAWSCredentials (:s3-access-key-id env) (:s3-secret-access-key env))))

(defn upload-file
  "Upload a file, return true if it worked."
  [local-file remote-name]
  (l/info "Uploading" local-file "to" remote-name ".")
  (let [^AmazonS3 client (aws-client)
        request (new PutObjectRequest (:archive-s3-bucket env) remote-name local-file)
        metadata (new ObjectMetadata)]
        (.setContentType metadata "application/json")
        (.withMetadata request metadata)
        (.putObject client request)
    
    ; S3 isn't transactional, may take a while to propagate. Try a few times to see if it uploaded OK, return success.
    (try-try-again {:sleep 5000 :tries 10 :return? :truthy?} (fn []
      (.doesObjectExist client  (:archive-s3-bucket env) remote-name)))))
