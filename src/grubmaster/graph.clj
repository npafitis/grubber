(ns grubmaster.graph
  (:require [grubmaster.node :refer :all]
            [clojure.set :as set]
            [clj-http.client :as client]
            [zeromq.zmq :as zmq]
            [clojure.core.async :as async]
            [utils.core :as utils]
            [clojure.tools.logging :as log]))

(declare init-recur)

(defn end-of-stream? [data]
  (= :end-of-stream data))

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

(defn get-node-closure [graph]
  (fn [id]
    (let [nodes (:nodes graph)]
      (first (filter (fn [node] (= (:id node) id)) nodes)))))

(defn get-vent [graph]
  (get-node graph :vent))

(defn vent-output-nodes [graph]
  (map (get-node-closure graph) (:out (get-vent graph))))

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

(defn create-vent [graph input-coll zmq-context]
  (let [outputs (vent-output-nodes graph)]
    (log/info "Creating Vent process")
    (async/go
      (with-open [vent-sock (zmq/socket zmq-context :push)]
        (for [output outputs]
          (let [url (:url output)
                port (:port output)
                full-uri (str "tcp://" url ":" port)]
            (log/info "Vent socket connecting to : " full-uri)
            (zmq/connect vent-sock full-uri)))
        (loop [value (first input-coll)
               rest-inp (rest input-coll)]
          (utils/write-sock vent-sock (or value :end-of-stream))
          (or (nil? value)
              (recur (first rest-inp) (rest rest-inp))))))))

(defn create-sink [zmq-context full-uri]
  (log/info "Creating Sink process")
  (async/go
    (with-open [sink-sock (doto (zmq/socket zmq-context :pull)
                            (zmq/bind full-uri))]
      (loop [data (utils/read-sock sink-sock)
             res []]
        (if (end-of-stream? data)
          res
          (recur (utils/read-sock sink-sock) (conj res data)))))))

(defn read-result [graph]
  (let [sink-chan (:sink-chan graph)]
    (async/<! sink-chan)))

(defrecord Graph [nodes input-coll sink-chan]
  IGraph
  (deploy! [this]
    (let [zmq-context (zmq/context 1)
          full-uri (str "tcp://*:" (:port (get-sink this)))]
      (let [sink-chan (create-sink zmq-context full-uri)
            _ (init-recur this)
            vent-chan (create-vent this (:input-coll this) zmq-context)]
        (assoc this :sink-chan sink-chan)))))

(defn create-graph [input-coll]
  (->Graph [{:id :vent :out []}                             ;; Vent node
            {:id   :sink :out []                            ;; Sink Node
             :url  (local-ip-address)
             :port (local-sink-port)}]
           input-coll
           nil))

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
    (let [node-src (get-node graph src)]
      (-> graph
          (update-node (add-node-relation node-src :out dst))))
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
  (log/info "Building Payload...")
  {:type    (:type node)
   :id      (:id node)
   :fn      (:fn node)
   :threads 1
   :out     (map #(str (->> %
                            (get-node graph)
                            (:url))
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
  (log/info "Recursively initializing graph nodes (end-to-begin)")
  (loop [graph graph
         reverse-graph-seq (reverse (bfs-seq graph))
         inits [:vent :sink]]
    (if (= (count inits) (- (count (:nodes graph)) 2))
      graph                                                 ;; terminal case
      (let [node-id (utils/debug (first reverse-graph-seq))
            rest-seq (vec (rest reverse-graph-seq))
            node (get-node graph node-id)
            outs (:out node)]
        (if (all-init? outs inits)
          (recur (update-node graph (init-node graph node)) rest-seq (conj inits node-id))
          (recur graph (conj rest-seq node-id) inits))))))