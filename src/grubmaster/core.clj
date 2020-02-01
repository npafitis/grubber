(ns grubmaster.core)
;;;;;;;;;;;;;;;;;
;; Graph Record
;;;;;;;;;;;;;;;;;

(defprotocol IGraph
  (deploy! [this]))

(defrecord Graph [nodes]
  IGraph
  (deploy! [this] this))

(defn create-graph []
  (->Graph '({:id :vent})))

;;;;;;;;;;;;;;;;;
;; Node Records
;;;;;;;;;;;;;;;;;

(defrecord Node [id fn url out type])


(defn create-map-node [{:keys [id transformer url]}]
  (->Node id transformer url nil :transform))

(defn create-collect-node [{:keys [id collector url]}]
  (->Node id collector url nil :collect))

;;;;;;;;;;;;;;;;;
;; Actions
;;;;;;;;;;;;;;;;;

(defn node-id-exists? [^Graph graph id]
  (not (empty? (filter #(= (:id %) id) (:nodes graph)))))

(defn node-exists? [^Graph graph
                    node]
  (node-id-exists? graph (:id node)))

(defn add-node [^Graph graph
                node]
  (if (node-exists? graph node)
    graph
    (update-in graph [:nodes] conj node)))

(defn valid-link? [^Graph graph
                   in out]
  (and (not (= in out))
       (and (node-id-exists? graph in)
            (node-id-exists? graph out))))


(defn add-node-relation [node id]
  (update-in node [:out] conj {:id  id
                               :url nil}))

(defn add-link [^Graph graph
                & [{:keys [src dst]}]]
  (if (valid-link? graph src dst)
    (update-in graph [:nodes]
               (fn [nodes]
                 (map
                   (fn [node] (if (= (:id node) src)
                                (add-node-relation node dst)
                                node))
                   nodes)))
    graph))

(def graph (-> (create-graph)
               (add-node (create-map-node
                           {:id 1 :transformer nil :url nil}))
               (add-node (create-map-node
                           {:id 2 :transformer nil :url nil}))
               (add-link {:src :vent :dst 1})
               (add-link {:src :vent :dst 2})
               (add-link {:src 1 :dst 2})
               (add-link {:src 1 :dst 1})                   ;;invalid
               (add-link {:src 1 :dst 3})                   ;;invalid
               (deploy!)))