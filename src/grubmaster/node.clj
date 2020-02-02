(ns grubmaster.node)

;;;;;;;;;;;;;;;;;
;; Node Records
;;;;;;;;;;;;;;;;;


(defrecord Node [id fn url port in out type])


(defn create-map-node [{:keys [id transformer url port]}]
  (->Node id transformer url port [] [] :transform))

(defn create-collect-node [{:keys [id collector url port]}]
  (->Node id collector url port [] [] :collect))

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


