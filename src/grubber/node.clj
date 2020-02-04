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

(defn threaded-pipeline! [emitter input-chan port]
  (log/info "Starting threaded pipeline...")
  (for [_ (range 0 (get-threads port))]
    (async/go-loop [data (async/<! input-chan)]
      (utils/write-sock emitter ((get-runner port) data))
      (or (end-of-stream? data)
          (recur (async/<! input-chan))))))

(defn single-pipeline! [emitter input-chan port]
  (log/info "Starting single-threaded pipeline...")
  (async/go-loop [data (async/<! input-chan)]
    (utils/write-sock emitter ((get-runner port) data))
    (or (end-of-stream? data)
        (recur (async/<! input-chan)))))

(defn run-node! [context emit-sock consume-sock port]
  (log/info "Running node at port: " port)
  (async/go
    (with-open [emitter (zmq/socket context emit-sock)
                consumer (doto (zmq/socket context consume-sock)
                           (zmq/bind (str "tcp://*:" port)))]

      (for [dst (:out (get-node-properties port))]
        (do
          (log/info "Emitter connecting to " dst)
          (zmq/connect emitter dst)))

      (let [input-chan (async/chan 1)
            threads (get-threads port)]
        (if (> threads 1)
          (threaded-pipeline! emitter input-chan port)
          (single-pipeline! emitter input-chan port))

        (loop [data (utils/read-sock consumer)]
          (log/info "Read " data "from consumer socket")
          (if (end-of-stream? data)
            ;; If end of stream then broadcast to all workers
            (do
              (log/info "End of stream received")
              (for [_ (range 0 threads)]
                (async/>! input-chan :end-of-stream)))

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
