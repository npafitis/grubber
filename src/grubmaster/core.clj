(ns grubmaster.core
  (:require [clojure.set :as set]))
;;;;;;;;;;;;;;;;;
;; Graph Record
;;;;;;;;;;;;;;;;;

(defn log [x]
  (do
    (prn x)
    x))

(defprotocol IGraph
  (deploy! [this]))

(defn get-node [graph id]
  (let [nodes (:nodes graph)]
    (first (filter (fn [node] (= (:id node) id)) nodes))))

(defn get-vent [graph]
  (get-node graph :vent))

(defn get-sink [graph]
  (get-node graph :sink))


(defn bfs-seq [graph]
  (loop [queue (set (:out (get-vent graph)))
         visited #{:sink}
         result []]
    (if (empty? queue)
      result
      (let [node-id (first queue)
            node (get-node graph node-id)
            queue (set (rest queue))]
        (recur
          ;; queue
          (into queue
                ;; filter out visited nodes and nodes already in queue
                (filter
                  (fn [output]
                    (not (some #(= % output) (set/union queue visited))))
                  (:out node)))
          ;; visited
          (conj visited node-id)
          ;; result
          (conj result node-id)))
      )))

(defrecord Graph [nodes]
  IGraph
  (deploy! [this] this))

(defn create-graph []
  (->Graph '({:id :vent :out []} {:id :sink :out []})))

;;;;;;;;;;;;;;;;;
;; Node Records
;;;;;;;;;;;;;;;;;

(defrecord Node [id fn url port out type])


(defn create-map-node [{:keys [id transformer url port]}]
  (->Node id transformer url port [] :transform))

(defn create-collect-node [{:keys [id collector url port]}]
  (->Node id collector url port [] :collect))

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
  (update-in node [:out] conj id))

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
                           {:id 1 :transformer nil :url nil :port nil}))
               (add-node (create-map-node
                           {:id 2 :transformer nil :url nil :port nil}))
               (add-node (create-map-node
                           {:id 3 :transformer nil :url nil :port nil}))
               (add-node (create-map-node
                           {:id 4 :transformer nil :url nil :port nil}))
               (add-node (create-map-node
                           {:id 5 :transformer nil :url nil :port nil}))
               (add-link {:src :vent :dst 1})
               (add-link {:src 2 :dst 5})
               (add-link {:src 1 :dst 2})
               (add-link {:src 1 :dst 3})
               (add-link {:src 5 :dst 4})
               (add-link {:src 4 :dst 5})
               (add-link {:src 4 :dst :sink})
               (deploy!)))

;; TODO: Undefined behaviour introduced when 2 output nodes of a node are also interlinked
