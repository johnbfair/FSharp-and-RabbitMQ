module RabbitRoutingSample

    open System
    open System.Xml
    open System.Xml.Linq
    open FSharp_RabbitMq.Types

    type TheGrid(count:int) =        
        let watch = new System.Diagnostics.Stopwatch()

        let supervisor = new Agent<System.Exception>(fun inbox ->
                            let rec Loop() =
                                async {
                                    let! err = inbox.Receive()
                                    printfn "An error occurred: %s" err.Message
                                    do! Loop() }
                            Loop()) |> Agent.start

        let timeKeeper = 
            new Agent<Status>(fun inbox ->
                let rec Loop() =
                    async {
                        let! msg = inbox.Receive()

                        match msg with
                        | Start -> watch.Start(); do! Loop()
                        | Stop -> 
                            watch.Stop()
                            printfn "Rabbit Routing processed %f messages/sec. Total Seconds: %f" ((float)count/watch.Elapsed.TotalSeconds) watch.Elapsed.TotalSeconds 
                        }
                Loop()) |> Agent.start

        let accountant = 
            new Agent<_>(fun inbox ->
                let rec Loop totalCount =
                    async {
                        let! msg = inbox.Receive()
                        
                        let totalCount' = totalCount - 1
                        
                        if (totalCount' = 0) then 
                            timeKeeper.Post Stop

                        do! Loop totalCount' }
                Loop count) |> Agent.start
                     
        member this.StartTimeKeeper() = timeKeeper.Post Start

        member this.ProcessMessage1Command ack (extMsg:string, tag:uint64) =
            (new Agent<Message1>(fun inbox ->
                async {
                    let! msg = inbox.Receive()
                    ack msg.MessageTag

                    accountant.Post()
                })
            |> Agent.reportErrorsTo supervisor 
            |> Agent.start)
            |> fun x-> 
                try 
                    let msg = Rock.Framework.Serialization.XmlObjectSerializer.XmlToObject<Message1>(extMsg)
                    msg.MessageTag <- tag
                    msg |> x.Post
                with
                | exn as Exception -> printfn "Exception: %s" exn.Message

        member this.ProcessMessage2Command ack (extMsg:string, tag:uint64) =
            (new Agent<Message2>(fun inbox ->
                async {
                    let! msg = inbox.Receive()
                    ack msg.MessageTag

                    accountant.Post()
                })
            |> Agent.reportErrorsTo supervisor 
            |> Agent.start)
            |> fun x-> 
                try
                    let msg = Rock.Framework.Serialization.XmlObjectSerializer.XmlToObject<Message2>(extMsg) 
                    msg.MessageTag <- tag
                    msg |> x.Post
                with
                | exn as Exception -> printfn "Exception: %s" exn.Message
