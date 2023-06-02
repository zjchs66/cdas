<template>
     <div class="block_query">
        <el-input v-model="input" size="mini" ></el-input>
    </div>
</template>
<style>
.block_query{
    width:100%;
}
.search-panel input {
    outline: none;
    border: solid 1px #ccc;
    background-color: transparent;
    color: #333;
    padding: 5px;
    /*width: 100px;*/
    border-radius: 2px !important;
}
</style>
<script>
    module.exports = {
        data() {
            return {
                input:'',
                width:'auto',
                like:true,
            }
        },
        methods:{
            init(self){
                if(this.config.like!==undefined)
                    this.like = this.config.like;
                else if(this.config.option!==undefined)
                    this.like = this.config.option=='like';
                if(self.runtime)
                    self.hideLabel();
                this.refresh();
            },
            property(self) {
                return [
                    {label:'文本框长度',key:'width',value:this.width},
                    {label:'模糊匹配',key:'like',value:this.like,type:'checkbox'}
                    
                ];
            },
            refresh(){
                // this.self.width(width);
                // if(this.width!='auto'){
                //     let node = document.querySelectorAll('.search-panel input');
                //     for (var i=0;i<node.length;i++){
                //         let node_id = node[i].parentNode.parentNode.parentNode.id;
                //         if (node_id==this.self.getRenderElement()[0].id){
                //             node[i].style.width=this.width;
                //         }
                //     }
                //     this.self.getRenderElement().css({width: this.width});
                // }
               
            },
            
            val(self,data) {
                if(data!==undefined&&data!=null){
                    if(typeof data==='object'){
                        this.input = data.value||"";   
                    }else{
                        this.input = data;
                    }
                }
                if(this.like){
                    return {value:this.input,operator:'like'}
                }else{
                    return this.input;
                }
            },
            toJson(self,result){
                delete result.app;
                if(this.like){
                    result.option="like";
                }else{
                    delete result.option;
                }
            }
        }
    }
</script>
