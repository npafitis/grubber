(ns grubmaster.graph
  (:require [grubmaster.node :refer :all]
            [clojure.set :as set]
            [clj-http.client :as client]))

(declare init-recur)
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
  (deploy! [this] this))

(defn create-graph []
  (->Graph [{:id :vent :out [] :in []} {:id :sink :out [] :in []}]))

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

(defn- build-payload [graph node]
  (-> {}
      (assoc-in [:type] (:type node))
      (assoc-in [:fn] (:fn node))
      (assoc-in [:out] (map #(str (->> %
                                       (get-node graph)
                                       (:url)
                                       (or "localhost"))
                                  ":"
                                  (->> %
                                       (get-node graph)
                                       (:port)))
                            (:out node)))))

(defn- init-node [graph node]
  (let [payload (build-payload graph node)
        res (client/post (str "http://" (:url node) ":" (or (:port node) "8080"))
                         {:body         (pr-str {:node payload})
                          :content-type :edn})]
    (:grubber-port (read-string (:body res)))))

(defn- init-recur [graph]
  (loop [graph graph
         reverse-graph-seq (reverse (bfs-seq graph))
         init [:vent :sink]]
    (if (= (count init) (count (:nodes graph)))
      graph                                                 ;; terminal case
      (let [node-id (first reverse-graph-seq)
            rest-seq (rest reverse-graph-seq)]
        (let [node (get-node graph node-id)
              outs (:out node-id)]
          (if (all-init? outs init)
            (do
              (init-node graph node)
              ())
            (recur graph (conj rest-seq node-id) init)))))))