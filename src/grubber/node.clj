(ns grubber.node
  (:require [zeromq.zmq :as zmq]
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

(defn get-fn [port]
  (:fn (get-node port)))

(defn get-runner [port]
  ((:runner (get-node-properties port)) (get-fn port)))

(defn update-node-context [port
                           ^NodeCtx node-ctx]
  (reset! node-context-map (assoc @node-context-map port node-ctx)))
;;;;;;;

(defn get-available-port [] (utils/get-free-port))

(defn transform [transformer]
  (fn [data] (transformer data)))

(defn collect [collector] (fn [data] nil))

(defn threaded-pipeline! [emitter input-chan port]
  (for [_ (range 0 (get-threads port))]
    (async/go-loop [data (async/<! input-chan)]
      (zmq/send-str emitter ((get-runner port) data))
      (or (end-of-stream? data)
          (recur (async/<! input-chan))))))

(defn single-pipeline! [emitter input-chan port]
  (async/go-loop [data (async/<! input-chan)]
    (zmq/send-str emitter ((get-runner port) data))
    (or (end-of-stream? data)
        (recur (async/<! input-chan)))))

(defn run-node! [context emit-sock consume-sock port]
  (with-open [emitter (zmq/socket context emit-sock)
              consumer (doto (zmq/socket context consume-sock)
                         (zmq/bind (str "tcp://*:" port)))]

    (for [dst (:out (get-node-properties port))]
      (zmq/connect emitter dst))

    (let [input-chan (async/chan 1)
          threads (:threads (get-node-properties port))]
      (if (> threads 1)
        (threaded-pipeline! emitter input-chan port)
        (single-pipeline! emitter input-chan port))

      (loop [data (zmq/receive-str consumer)]
        (if (end-of-stream? data)
          ;; If end of stream then broadcast to all workers
          (for [_ (range 0 threads)]
            (async/>! input-chan :end-of-stream))
          (do
            (async/>! input-chan data)
            (recur (zmq/receive-str consumer))))))))

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
    (async/go (run-node! @context emit-sock consume-sock port))
    port))
