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


(defn node-id-exists? [graph id]
  (not (empty? (filter #(= (:id %) id) (:nodes graph)))))

(defn node-exists? [graph
                    ^Node node]
  (node-id-exists? graph (:id node)))

(defn add-node [graph
                ^Node node]
  (if (node-exists? graph node)
    graph
    (update-in graph [:nodes] conj node)))
