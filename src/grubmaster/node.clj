(ns grubmaster.node)

;;;;;;;;;;;;;;;;;
;; Node Records
;;;;;;;;;;;;;;;;;


(defrecord Node [id fn url port out type threads])


(defn create-map-node [{:keys [id transformer url port threads]}]
  (->Node id transformer (or url "localhost") (or port "8080") [] :map (or threads 1)))

(defn create-reduce-node [{:keys [id reducer url port threads]}]
  (->Node id reducer (or url "localhost") (or port "8080") [] :reduce (or threads 1)))

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


