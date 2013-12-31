module RabbitCBRSample

    open System
    open System.Xml
    open System.Xml.Linq
    open FSharp_RabbitMq.Types

    type TheGrid(ack, message1CmdQ: RabbitMqPublisher, message2CmdQ: RabbitMqPublisher, count:int) =        
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
                            printfn "Rabbit CBR processed %f messages/sec. Total Seconds: %f" ((float)count/watch.Elapsed.TotalSeconds) watch.Elapsed.TotalSeconds 
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
                Loop (count*2)) |> Agent.start
                     
        member this.StartTimeKeeper() = timeKeeper.Post Start

        member this.RouteCommand(extMsg:string, tag:uint64) =
            (new Agent<Command>(fun inbox ->
                async {
                        let watch = System.Diagnostics.Stopwatch.StartNew()

                        let! msg = inbox.Receive()
                        let msgType =
                            match System.Text.RegularExpressions.Regex.Match(msg.Message, "<(?<tag>\w*)>") with
                            | x when x.Success -> x.Groups.[1].Value
                            | _ -> raise (new Exception "Couldn't determine message type (null root)")

                        match msgType.ToLower() with
                        | "message1" ->                           
                            (new Agent<Command>(fun inbox ->
                                                    async {
                                                        let! x = inbox.Receive()
                                                        let! m = message1CmdQ.Send x.Message None
                                                        ack x.Tag
                                                    }) |> Agent.reportErrorsTo supervisor |> Agent.start) 
                            |> fun x-> x.Post msg

                        | "message2" -> 
                            (new Agent<Command>(fun inbox ->
                                                    async {
                                                        let! x = inbox.Receive()
                                                        let! m = message2CmdQ.Send x.Message None
                                                        ack x.Tag
                                                    }) |> Agent.reportErrorsTo supervisor |> Agent.start) 
                            |> fun x-> x.Post msg
                        | _ -> ack msg.Tag; raise (new Exception "Invalid Command") 
                        
                        accountant.Post()
                        })
                |> Agent.reportErrorsTo supervisor 
                |> Agent.start)
                |> fun x-> x.Post {Message=extMsg; Tag=tag}

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
                let msg = Rock.Framework.Serialization.XmlObjectSerializer.XmlToObject<Message1>(extMsg) 
                msg.MessageTag <- tag
                msg |> x.Post

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
                let msg = Rock.Framework.Serialization.XmlObjectSerializer.XmlToObject<Message2>(extMsg) 
                msg.MessageTag <- tag
                msg |> x.Post
