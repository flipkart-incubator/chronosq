ChronosQ
-----------

chronosQ is a scheduler which runs client specific tasks on particular object when the object expires at a particular given time when being injected.
chronosQ comprises of two modules

        chronosQ-client    - Part of clientApp to send data to library.
        chronosQ-ha-worker - Individual module runs separately which reaps out data from datastore and put in queue                               available for consumer

#### Architecture
![https://docs.google.com/presentation/d/1ykneLhJIf6r1cydbYVGV2dwtZjSNCW5lz7EbKHJDdBM/edit#slide=id.plink](https://i.imgur.com/upN61tn.jpg "Architecture")

        
#### Features

   - Configurable TimeBucket for objects to fit in
   - Ha-worker 
   - Pluggable datastore and sink impl

#### Usecases

   - Can be used as a scheduler when custom actions to be taken when object expires at specified time.
   
