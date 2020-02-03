(ns grubmaster.node)

;;;;;;;;;;;;;;;;;
;; Node Records
;;;;;;;;;;;;;;;;;


(defrecord Node [id fn url port out type threads])


(defn create-map-node [{:keys [id transformer url port threads]}]
  (->Node id transformer (or url "localhost") (or port "8080") [] :transform (or threads 1)))

(defn create-collect-node [{:keys [id collector url port threads]}]
  (->Node id collector (or url "localhost") (or port "8080") [] :collect (or threads 1)))

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


