(ns caribou.image
  (:import [java.io FileOutputStream]
           [java.awt Color]
           [java.awt.image BufferedImage BufferedImageOp]
           [javax.imageio ImageIO ImageReader]
           [org.imgscalr Scalr Scalr$Method]))

(defn normalize-format
  "Normalizes the string representing a format. Basically, applies toUpperCase and takes JPG to JPEG."
  [#^String format]
  (let [frmt (.toUpperCase format)]
    (if (= frmt "JPG") "JPEG" frmt)))

(defmulti get-format class)

(defmethod get-format String
  [#^String filename]
  (let [idx (inc (.lastIndexOf filename (int \.)))]
    (.substring filename idx)))

(defmethod get-format ImageReader
  [#^ImageReader reader]
  (.getFormatName reader))

(def get-normalized-format
  (comp normalize-format get-format))
    
(defmacro assure-type
  [t]
  (let [type (symbol (str "BufferedImage/TYPE_INT_"
                          (.toUpperCase (str t))))
        fn-name (symbol (str "assure-" t))]
    `(defn- ~fn-name
       [~'img]
       (let [nbi# (BufferedImage. (.getWidth ~'img)
                                  (.getHeight ~'img)
                                  ~type)]
         (doto (.createGraphics nbi#)
           (.drawImage ~'img 0 0 nil)
           (.dispose))
         nbi#))))

(assure-type argb)
(assure-type rgb)

(defn create-empty-canvas
  "Creates an empty BufferedImage with the predefined color."
  ([width height]
     (create-empty-canvas width height Color/WHITE))

  ([width height color]
     (let [buf-img (BufferedImage. width height BufferedImage/TYPE_INT_ARGB)]
       (doto (.getGraphics buf-img)
         (.setBackground color)
         (.clearRect 0 0 width height)
         (.dispose))
       buf-img)))

(defn create-new-canvas-for-image
  "Creates a new canvas an copies the image to it"
  [img]
  (let [buf-img (create-empty-canvas (:width img) (:height img))]
    (doto (.getGraphics buf-img)
      (.drawImage (:image img) 0 0 nil))
    buf-img))

(defstruct image :image :format :width :height)

(defmulti create-image
  "Creates a new image from BufferedImage or ImageReader"
  (fn [x & xs]
    (class x)))

(defmethod create-image BufferedImage
  [buf-img]
  (struct image (assure-argb buf-img) "JPEG" (.getWidth buf-img) (.getHeight buf-img)))

(defmethod create-image ImageReader
  [reader]
  (let [frmt (get-normalized-format reader)
	img (assure-argb (.read reader 0))
        width (.getWidth img)
        height (.getHeight img)]
    (struct image img frmt width height)))

(defmethod create-image :default
  ([width height]
     (create-image (create-empty-canvas width height)))
  ([width height color]
     (create-image (create-empty-canvas width height color))))

(defmulti read-image
  "Reads an image from different resources. Look at ImageIO.createImageInputStream for more info"
  class)

(defmethod read-image String
  [s]
  (read-image (java.io.File. s)))

(defmethod read-image :default
  [origin]
  (when origin
    (let [stream (ImageIO/createImageInputStream origin)]
      (when stream
	(with-open [s stream]
	  (let [r (last (iterator-seq (ImageIO/getImageReaders s)))]
	    (when r
	      (.setInput r stream)
	      (create-image r))))))))

(defn write-buffered-image
  [buf-img uri]
  "Writes a BufferedImage to a File whose path is uri"
  (with-open [file (FileOutputStream. uri)]
    (ImageIO/write buf-img (get-normalized-format uri) file)))

(defn write-image
  "Write the image img to uri"
  [img uri]
  (let [i (if (= "JPEG" (get-normalized-format uri))
            (assure-rgb (:image img))
            (:image img))]
    (try
      (write-buffered-image i uri)
      true
      (catch Exception e (println (str e))))))
        ;; false))))

(defn resize
  "Resizes the image specified by filename according to the supplied options
  (:width or :height), saving to file new-filename.  This function retains
  the aspect ratio of the original image."
  [filename new-filename opts]
  (let [img (read-image filename)
        width (img :width)
        height (img :height)
        ratio (/ (float width) (float height))
        target-width (or (opts :width) (* (opts :height) ratio))
        target-height (or (opts :height) (/ (opts :width) ratio))
        larger (max target-width target-height)
        scaled (Scalr/resize (img :image) Scalr$Method/ULTRA_QUALITY (int larger) (into-array BufferedImageOp []))]
    (write-image {:image scaled} new-filename)))



