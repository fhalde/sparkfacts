(ns sparkfacts.core
  (:require [clojure.java.io :as io]
            [sparkfacts.massager :as msgr]
            [clojure.walk :as cw]
            [clojure.string :as cs]
            [clojure.data.json :as cdj]
            [xtdb.api :as xt])
  (:import [java.nio.file Files]))


(def xtdb-node nil)

(defn start-xtdb! [base-dir]
  (letfn [(kv-store [dir]
            {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                        :db-dir (io/file dir)
                        :sync? false}})]
    (xt/start-node
     {:xtdb/tx-log (kv-store (str base-dir "/dev/tx-log"))
      :xtdb/document-store (kv-store (str base-dir "/dev/doc-store"))
      :xtdb/index-store (kv-store (str base-dir "/dev/index-store"))})))


(defn stop-xtdb! []
  (.close xtdb-node))


(defn init
  []
  (let [dir (.toString
             (.toAbsolutePath
              (Files/createTempDirectory "prefix"
                                         (make-array java.nio.file.attribute.FileAttribute 0))))]
    (alter-var-root #'xtdb-node (constantly (start-xtdb! dir)))))


(defn fixkeys
      [s]
      (if (keyword? s)
        (keyword (cs/replace (clojure.string/lower-case (name s)) #" " "_"))
        s))


(defn populate
  [cid logfile]
  (xt/submit-tx xtdb-node [[::xt/put
                            {:xt/id "merge"
                             :xt/fn '(fn [ctx eid changes vt]
                                       (let [db (xtdb.api/db ctx)
                                             e  (xtdb.api/entity db eid)
                                             op [::xt/put (merge e changes)]]
                                         [(into op vt)]))}]])

  (xt/submit-tx xtdb-node [[::xt/put
                            {:xt/id "assoc"
                             :xt/fn '(fn [ctx eid k v vt]
                                       (let [db (xtdb.api/db ctx)
                                             e  (xtdb.api/entity db eid)
                                             op [::xt/put (assoc e k v)]]
                                         [(into op vt)]))}]])


  (xt/submit-tx xtdb-node [[::xt/put
                            {:xt/id "conj-k"
                             :xt/fn '(fn [ctx eid k v vt]
                                       (let [db (xtdb.api/db ctx)
                                             e  (xtdb.api/entity db eid)
                                             op [::xt/put (update e k (fnil conj #{}) v)]]
                                         [(into op vt)]))}]])


  (xt/submit-tx xtdb-node [[::xt/put
                            {:xt/id "inc"
                             :xt/fn '(fn [ctx eid k vt]
                                       (let [db (xtdb.api/db ctx)
                                             e  (xtdb.api/entity db eid)
                                             op [::xt/put (update e k inc)]]
                                         [(into op vt)]))}]])
  (xt/sync xtdb-node)

  (->> (slurp logfile)
       (clojure.string/split-lines)
       (mapv (comp
              (partial msgr/massager cid)
              (fn [x] (cw/postwalk fixkeys x))
              (fn [x] (cdj/read-json x true))))
       (filterv (fn [x] (not= x {:undefined true})))
       (run! (partial xt/submit-tx xtdb-node)))

  (xt/sync xtdb-node))



(defn q [query & args]
  (apply xt/q (xt/db xtdb-node) query args))


(defn get-ent
  [id]
  (xt/entity (xt/db xtdb-node) id))


(defn number-of-stages
  [cid job-id]
  (ffirst (q '{:find [(count ?stage)]
               :in [cid job-id]
               :where [[?app :xt/id cid]
                       [?app :app/jobs ?job]
                       [?job :job/id job-id]
                       [?job :job/stages ?stage]]}
             cid
             job-id)))
