(ns event-data-deposit-stasher.util
  "Utility functions"
  (:require [config.core :refer [env]])
  (:require [clojure.tools.logging :as l])
  (:import [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
           [com.amazonaws.auth BasicAWSCredentials]
           [com.amazonaws.services.s3.model GetObjectRequest PutObjectRequest ObjectMetadata])
  (:import [redis.clients.jedis Jedis])
  (:import [java.util UUID]
           [java.net URL])
  (:require [robert.bruce :refer [try-try-again]]
            [crossref.util.doi :as cr-doi])
  (:gen-class))


(defn get-aws-client
  []
  (new AmazonS3Client (new BasicAWSCredentials (:s3-access-key-id env) (:s3-secret-access-key env))))

(def aws-client (delay (get-aws-client)))

(def doi-resolvers #{"doi.org" "dx.doi.org"})

(defn is-doi
  [pid]
    (let [domain (.toLowerCase (.getHost (new URL pid)))]
      (doi-resolvers domain)))

(defn normalize-pid
  "Normalize a PID if it's a DOI, or don't touch it."
  [pid]
    (if (is-doi pid)
      (cr-doi/normalise-doi pid)
      pid))

(defn upload-file
  "Upload a file, return true if it worked."
  [local-file remote-name content-type]
  (l/info "Uploading" local-file "to" remote-name ".")
  (let [request (new PutObjectRequest (:archive-s3-bucket env) remote-name local-file)
        metadata (new ObjectMetadata)]
        (.setContentType metadata content-type)
        (.withMetadata request metadata)
        (.putObject @aws-client request)
    
    ; S3 isn't transactional, may take a while to propagate. Try a few times to see if it uploaded OK, return success.
    (try-try-again {:sleep 5000 :tries 10 :return? :truthy?} (fn []
      (.doesObjectExist @aws-client  (:archive-s3-bucket env) remote-name)))))


(defn transform-deposit
  [item]
  """Transform deposit into an event schema. Temporary workaround until functionality is in Lagotto."""
  (let [message-action (get item "message_action" "add")
      subj-id (get item "subj_id")
      obj-id (get item "obj_id")
      subj-is-doi (is-doi subj-id)
      obj-is-doi (is-doi obj-id)
      normalized-subj-id (normalize-pid subj-id)
      normalized-obj-id (normalize-pid obj-id)
      base-result {:id (get item "id")
                   :message_action message-action
                   :subj_id normalized-subj-id
                   :obj_id normalized-obj-id
                   :occurred_at (get item "occurred_at")
                   :relation_type_id (get item "relation_type_id")
                   :source_id (get item "source_id")
                   :timestamp (get item "timestamp")
                   :total (get item "total" 1)}
      ; Don't include subj or obj metadata when the PID is a DOI.
      result-subj-metadata (when-not subj-is-doi {:subj (get item "subj")})
      result-obj-metadata (when-not obj-is-doi {:obj (get item "obj")})

      result (merge base-result result-subj-metadata result-obj-metadata)]
    result))
