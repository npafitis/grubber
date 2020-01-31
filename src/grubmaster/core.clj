(ns grubmaster.core
  (:import (java.util List)))
;;;;;;;;;;;;;;;;;
;; Graph Record
;;;;;;;;;;;;;;;;;

(defrecord Graph [^List nodes
                  ^List links])

;;;;;;;;;;;;;;;;;
;; Node Records
;;;;;;;;;;;;;;;;;

(defprotocol Node
  (node-id [this])
  (node-fn [this]))

(defrecord MapNode [id fn]
  Node
  (node-id [this] (:id this))
  (node-fn [this] (:fn this)))

(defrecord ReduceNode [id fn]
  Node
  (node-id [this] (:id this))
  (node-fn [this] (:fn this)))

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


(def graph (-> (->Graph nil nil)
               (add-node (->MapNode 1 nil))
               (add-node (->MapNode 2 nil))))