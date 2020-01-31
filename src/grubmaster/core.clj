(ns grubmaster.core
  (:import (java.util List)))
;;;;;;;;;;;;;;;;;
;; Graph Record
;;;;;;;;;;;;;;;;;

(defprotocol IGraph
  (deploy! [this]))

(defrecord Graph [^List nodes]
  IGraph
  (deploy! [this] this))

;;;;;;;;;;;;;;;;;
;; Node Records
;;;;;;;;;;;;;;;;;

(defprotocol Node
  (node-id [this])
  (node-fn [this])
  (node-in [this])
  (node-out [this]))

(defrecord MapNode [id fn
                    ^List in
                    ^List out]
  Node
  (node-id [this] (:id this))
  (node-fn [this] (:fn this))
  (node-in [this] (:in this))
  (node-out [this] (:out this)))


(defn create-map-node [id fn]
  (->MapNode id fn nil nil))

(defrecord ReduceNode [id fn
                       ^List in
                       ^List out]
  Node
  (node-id [this] (:id this))
  (node-fn [this] (:fn this))
  (node-in [this] (:in this))
  (node-out [this] (:out this)))

(defn create-reduce-node [id fn]
  (->ReduceNode id fn nil nil))
;;;;;;;;;;;;;;;;;
;; Link Record
;;;;;;;;;;;;;;;;;

(defrecord Link [in out type])

;;;;;;;;;;;;;;;;;
;; Actions
;;;;;;;;;;;;;;;;;

(defn node-id-exists? [^Graph graph id]
  (not (empty? (filter #(= (node-id %) id) (:nodes graph)))))

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

(defn add-link [^Graph graph
                id-src id-dst]
  (if (valid-link? graph id-src id-dst)
    (update-in graph [:nodes] (fn [nodes]
                                (map (fn [node]
                                       (cond (= (node-id node) id-src) (update-in node [:out] conj id-src)
                                             (= (node-id node) id-dst) (update-in node [:in] conj id-dst)
                                             :else node)) nodes)))
    graph))

(def graph (-> (->Graph nil)
               (add-node (create-map-node 1 nil))
               (add-node (create-map-node 2 nil))
               (add-node (create-map-node 2 nil))
               (add-link 1 2)
               (add-link 1 1)                               ;;invalid
               (add-link 1 3)                               ;;invalid
               (deploy!)))