(ns grubmaster.node)

;;;;;;;;;;;;;;;;;
;; Node Records
;;;;;;;;;;;;;;;;;


(defrecord Node [id fn url port out type threads])


(defn create-map-node [{:keys [id mapperf url port threads]
                        :or   {url "localhost" port 8080 threads 1}}]
  (->Node id mapperf url port [] :map threads))

(defn create-reduce-node [{:keys [id reducerf url port threads]
                           :or   {url "localhost" port 8080 threads 1}}]
  (->Node id reducerf url port [] :reduce threads))

(defn add-node-relation [^Node node
                         port
                         id]
  (update-in node [port] conj id))

(defn update-node-url [^Node node
                       url]
  (assoc-in node [:url] url))

(defn update-node-port [^Node node
                        port]
  (assoc-in node [:port] port))

(defn update-node-property [^Node node
                            property value]
  (assoc-in node [property] value))


