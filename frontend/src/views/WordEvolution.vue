<template>
    <v-container>
      <v-row
      justify="center"
      class="mt-10">
        <h1>Word Evolution</h1>
      </v-row>
      <v-row
      justify="center"
      class="mt-10">
        <v-btn class="mr-5" v-for="btn in words"  :key="btn.index"
          :id="btn"
          rounded
          color="primary"
          @click="wichButton($event)"
          dark
        >
          {{ btn }}
        </v-btn>
      </v-row>
      <v-row
      justify="center"
      class="mt-10">
      <div v-if="wordEvolutionData.length >0" >
        <line-chart :chartData="wordEvolutionData" :chartColors="chartColors" :options="chartOptions" :height="600" :width="900" :label="label"></line-chart>
      </div>
      </v-row>
    </v-container>
</template>

<script>
import axios from 'axios';
import LineChart from '../components/LineChart';

export default {
  name: 'WordEvolution',
  components: {
    LineChart
  },
  data: () => ({
    wordEvolutionData: [],
    chartColors: {
      borderColor: "rgba(250, 30, 30, 1)",
      pointBorderColor: "#656176",
      pointBackgroundColor: "#F8F1FF",
      backgroundColor: "rgba(231, 43, 43, 0.4)",
    },
    chartOptions: {
      responsive: true,
    },
    words: [],
    label: "",
  }),
  mounted(){
    this.generateButton();
  },
  methods: {
    async wichButton(){
      let targetId = event.currentTarget.id;
      this.label = targetId;
      const response = await axios("http://localhost:7000/word_pop");
      this.wordEvolutionData = [];
      await this.timeout(50);
      const values = response.data[targetId]
      values.splice(values.length-1,1);
      console.log(values);
      this.wordEvolutionData = values;
    },
    async generateButton(){
      const response = await axios("http://localhost:7000/word_pop");
      this.words = response.data.words

    },
    async timeout(ms) {
      return new Promise(resolve => setTimeout(resolve, ms));
    } 
  },
}
</script>
