/**
 *
 * @author http://www.turnguard.com/turnguard
 */
ApplicationState = function(aCustomEvents){
        this.state = new Object();
        this.onSet = new YAHOO.util.CustomEvent('set');
        this.onRemove = new YAHOO.util.CustomEvent('remove');
        this.customSetEvents = new Object();
        this.customRemoveEvents = new Object();
        if(aCustomEvents!==undefined && (typeof aCustomEvents === "object")){
                for(var i = 0; i < aCustomEvents.length; i++){
                                this.customSetEvents[aCustomEvents[i]] = new YAHOO.util.CustomEvent("set");
                                this.customRemoveEvents[aCustomEvents[i]] = new YAHOO.util.CustomEvent("remove");
                }
        }
};
ApplicationState.prototype = {
        set:function(sKey, oValue, bSilent, additionalObject){            
                if(bSilent===undefined || bSilent==null){ bSilent=false;}
                var oldValue = this.state[sKey];
                this.state[sKey] = oValue;
                if(!bSilent){
                        this.onSet.fire(sKey, oValue, oldValue, additionalObject);
                        if(this.customSetEvents[sKey]!==undefined){                            
                                this.customSetEvents[sKey].fire(sKey, oValue, oldValue, additionalObject);                                
                        }
                }
        },
        get: function(sKey){
                return this.state[sKey];
        },
        remove: function(sKey){
                var oldValue = this.state[sKey];
                delete this.state[sKey];
                this.onRemove.fire(sKey, oldValue);
                if(this.customRemoveEvents[sKey]!==undefined){
                        this.customRemoveEvents[sKey].fire(sKey, oldValue);
                }
        },
        addCustomEvent: function(sKey){
                if(this.customSetEvents[sKey]===undefined && this.customRemoveEvents[sKey]===undefined){
                        this.customSetEvents[sKey] = new YAHOO.util.CustomEvent("set");
                        this.customRemoveEvents[sKey] = new YAHOO.util.CustomEvent("remove");
                }
        },
        setKeyListener:function(sKey, fFunction, scope, type){
                if(type===undefined){ type="set";}
                if(type=="set" || type=="both"){
                        this.customSetEvents[sKey].subscribe(fFunction, scope);
                }
                if(type=="remove" || type=="both"){
                        this.customRemoveEvents[sKey].subscribe(fFunction, scope);
                }
        }
};
ApplicationState.newInstance = function(aCustomEvents){
        return new ApplicationState(aCustomEvents);
};