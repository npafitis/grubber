(ns grubber.node
  (:require [zeromq.zmq :as zmq]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [utils.core :as utils]))

(defn end-of-stream? [data]
  (= data :end-of-stream))

(defrecord NodeCtx [node node-properties threads acc])

(def node-context-map (atom {}))

(defn get-node [port]
  (:node (@node-context-map port)))

(defn get-node-properties [port]
  (:node-properties (@node-context-map port)))

(defn get-threads [port]
  (:threads (get-node port)))

(defn get-node-fn [port]
  (:fn (get-node port)))

(defn get-acc [port]
  (:acc (@node-context-map port)))

(defn get-runner [port]
  (let [node-properties (get-node-properties port)
        runner (:runner node-properties)
        node-fn (get-node-fn port)]
    (runner node-fn nil)))

(defn update-node-context [port
                           ^NodeCtx node-ctx]
  (reset! node-context-map (assoc @node-context-map port node-ctx)))
;;;;;;;

(defn get-available-port [] (utils/get-free-port))

(defn transform [transformer _]
  (fn [data] ((eval transformer) data)))

(defn collect [collector port]
  (fn [data]
    ((eval collector) (get-acc port) data)))

(defn emitter-connect! [emitter port]
  (loop [outs (:out (get-node port))]
    (let [out (first outs)]
      (or (nil? out)
          (do
            (log/info "Emitter connecting to " out)
            (zmq/connect emitter (str "tcp://" out)))))))

(defn single-pipeline! [context emit-sock input-chan port]
  (log/info "Starting single-threaded pipeline...")
  (async/go
    (with-open [emitter (zmq/socket context emit-sock)]
      (emitter-connect! emitter port)
      (loop [data (async/<! input-chan)]
        (if (end-of-stream? data)
          (do
            (log/info "Thread Received end of stream")
            (utils/write-sock emitter :end-of-stream))
          (do
            (utils/write-sock emitter ((get-runner port) data))
            (recur (async/<! input-chan))))))))

(defn threaded-pipeline! [context emit-sock input-chan port]
  (log/info "Starting threaded pipeline...")
  (for [_ (range 0 (get-threads port))]
    (single-pipeline! context emit-sock input-chan port)))

(defn run-node! [context emit-sock consume-sock port]
  (log/info "Running node at port: " port)
  (async/go
    (with-open [consumer (doto (zmq/socket context consume-sock)
                           (zmq/bind (str "tcp://*:" port)))]

      (let [input-chan (async/chan 1)
            threads (get-threads port)]
        (if (> threads 1)
          (threaded-pipeline! context emit-sock input-chan port)
          (single-pipeline! context emit-sock input-chan port))

        (loop [data (utils/read-sock consumer)]
          (log/info "Read " data "from consumer socket")
          (if (end-of-stream? data)
            ;; If end of stream then broadcast to all workers
            (do
              (log/info "End of stream received")
              (loop [counter 0]
                (or (= counter threads)
                    (do
                      (log/info "Passing :end-of-stream to input channel")
                      (async/>! input-chan :end-of-stream)
                      (recur (inc counter))))))

            (do
              (log/info "Pushing data to input channel: (Data " data ")")
              (async/>! input-chan data)
              (recur (utils/read-sock consumer)))))
        ))))

(def node-properties {:transform {:runner       #'transform
                                  :emit-sock    :push
                                  :consume-sock :pull}
                      :collect   {:runner       #'collect
                                  :consume-sock :pull}})

(defn run-grubber! [node context]
  ;; node contains :type :fn and :out
  (let [properties ((:type node) node-properties)
        emit-sock (:emit-sock properties)
        consume-sock (:consume-sock properties)
        port (get-available-port)]
    (update-node-context port (->NodeCtx node properties nil nil))
    (run-node! @context emit-sock consume-sock port)
    port))
