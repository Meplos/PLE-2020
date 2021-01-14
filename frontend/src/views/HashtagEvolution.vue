<template>
    <v-container>
      <v-row
      justify="center"
      class="mt-10">
        <h1>Hashtag Evolution</h1>
      </v-row>
      <v-row
      align="center"
      justify="center"
      class="mt-10">
        <v-btn class="mr-5"
          rounded
          id="covid19"
          color="primary"
          @click="wichButton($event)"
          dark
        >
          covid19
        </v-btn>
        <v-btn class="mr-5"
          rounded
          id="bts"
          color="primary"
          @click="wichButton($event)"
          dark
        >
          bts
        </v-btn>
        <div id="input">
          <v-text-field label="Search hashtag (without #)"
            @keydown.enter="searchHashtag($event)"
          ></v-text-field>
        </div>
      </v-row>
      <v-row
      justify="center"
      class="mt-10">
      <div v-if="hashtagEvolutionData.length >0" >
        <line-chart :chartData="hashtagEvolutionData" :chartColors="chartColors" :options="chartOptions" :height="600" :width="900" :label="label"></line-chart>
      </div>
      </v-row>
    </v-container>
</template>

<script>
import axios from 'axios';
import LineChart from '../components/LineChart';

export default {
  name: 'HashtagEvolution',
  components: {
    LineChart
  },
  data: () => ({
    hashtagEvolutionData: [],
    chartColors: {
      borderColor: "rgba(30, 250, 30, 1)",
      pointBorderColor: "#656176",
      pointBackgroundColor: "#F8F1FF",
      backgroundColor: "rgba(43, 231, 43, 0.4)",
    },
    chartOptions: {
      responsive: true,
    },
    label: "",
  }),
  methods: {
    async wichButton(event){
      let targetId = event.currentTarget.id;
      await this.getData(targetId);
    },
    async timeout(ms) {
      return new Promise(resolve => setTimeout(resolve, ms));
    },
    async searchHashtag(event){
      let word = event.srcElement._value;
      word.toLowerCase();
      await this.getData(word);
    },
    async getData(word){
      this.label = word;
      const response = await axios("http://localhost:7000/hashtag_pop/"+word);
      const days = response.data.map(day => {
        let col = day.column.split(":")
        return col[1];
      });
      const value = response.data.map(day => day.$);
      const result = [];
      for (let index = 0; index < days.length-1; index++) {
        result.push({date: days[index], total: value[index]})
      }
      this.hashtagEvolutionData = [];
      await this.timeout(50);
      this.hashtagEvolutionData = result;
    },
  },
}
</script>

<style scoped>
#input{
  width: 250px;
  }
</style>
