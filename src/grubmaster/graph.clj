(ns grubmaster.graph
  (:require [grubmaster.node :refer :all]
            [clojure.set :as set]
            [clj-http.client :as client]))

(declare init-recur)
;;;;;;;;;;;;;;;;;;;
;; IP Utils
;;;;;;;;;;;;;;;;;;;
(defn local-ip-address []
  "127.0.0.1")

(defn local-sink-port []
  "5556")
;;;;;;;;;;;;;;;;;
;; Graph Record
;;;;;;;;;;;;;;;;;

(defprotocol IGraph
  (deploy! [this]))

(defn get-node
  "Returns node of the graph that has given id."
  [graph id]
  (let [nodes (:nodes graph)]
    (first (filter (fn [node] (= (:id node) id)) nodes))))

(defn get-vent [graph]
  (get-node graph :vent))

(defn get-sink [graph]
  (get-node graph :sink))

(defn update-node
  "Returns graph with modified node (node is searched by id)"
  [graph
   node]
  (update-in graph [:nodes]
             (fn [nodes]
               (map #(cond (= (:id %) (:id node)) node :else %) nodes))))

(defn- bfs-seq
  "Returns a sequence of nodes by traversing the Graph breadth-first"
  [graph]
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
          (conj result node-id))))))

(defrecord Graph [nodes]
  IGraph
  (deploy! [this] (init-recur this)))

(defn create-graph []
  (->Graph [{:id :vent :out [] :in []}                      ;; Vent node
            {:id   :sink :out [] :in []                     ;; Sink Node
             :url  (local-ip-address)
             :port (local-sink-port)}]))

;;;;;;;;;;;;;;;;;
;; Actions
;;;;;;;;;;;;;;;;;
(defn node-id-exists?
  "Returns true if a node with given id exists in the graph."
  [graph id]
  (not (empty? (filter #(= (:id %) id) (:nodes graph)))))

(defn node-exists? [graph
                    node]
  "Returns true if a node with the same id as the given node's id exists in the graph."
  (node-id-exists? graph (:id node)))

(defn valid-link? [^Graph graph
                   in out]
  (and (not (= in out))
       (and (node-id-exists? graph in)
            (node-id-exists? graph out))))

(defn add-link [^Graph graph
                & [{:keys [src dst]}]]
  (if (valid-link? graph src dst)
    (let [node-src (get-node graph src)
          node-dst (get-node graph dst)]
      (-> graph
          (update-node (add-node-relation node-src :out dst))
          (update-node (add-node-relation node-dst :in src))))
    graph))

(defn add-node [graph
                node]
  (if (node-exists? graph node)
    graph
    (update-in graph [:nodes] conj node)))


;;;;;;;;;;;;;;;;;;;;;;;;
;; Initialization
;;;;;;;;;;;;;;;;;;;;;;;;
(defn- init? [init]
  (fn [node] (some #(= % node) init)))

(defn- all-init? [nodes init]
  (empty? (filter (comp not (init? init)) nodes)))

(defn- build-payload
  "Building payload to send to grubber service."
  [graph node]
  {:type    (:type node)
   :id      (:id node)
   :fn      (:fn node)
   :threads 1
   :out     (map #(str (->> %
                            (get-node graph)
                            (:url)
                            (or "localhost"))
                       ":"
                       (->> %
                            (get-node graph)
                            (:port)))
                 (:out node))})

(defn- request-init [graph node]
  (let [payload (build-payload graph node)
        res (client/post (str "http://" (:url node) ":" (or (:port node) "8080"))
                         {:body         (pr-str {:node payload})
                          :content-type :edn})]
    (read-string (:body res))))

(defn- init-node [graph node]
  (update-node-port node (:grubber-port (request-init graph node))))

(defn- init-recur [graph]
  (loop [graph graph
         reverse-graph-seq (reverse (bfs-seq graph))
         inits [:vent :sink]]
    (if (= (count inits) (count (:nodes graph)))
      graph                                                 ;; terminal case
      (let [node-id (first reverse-graph-seq)
            rest-seq (vec (rest reverse-graph-seq))
            node (get-node graph node-id)
            outs (:out node)]
        (if (all-init? outs inits)
          (recur (update-node graph (init-node graph node)) rest-seq (conj inits node-id))
          (recur graph (conj rest-seq node-id) inits))))))