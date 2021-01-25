<template>
    <v-container>
      <v-row
      justify="center"
      class="mt-10">
        <h1>Location Popularity</h1>
      </v-row>
      <v-row
      justify="center"
      class="mt-10">
        <v-btn class="mr-5"
          rounded
          color="primary"
          @click="wichButton"
          id="fr"
          dark
        >
          fr
        </v-btn>
        <v-btn class="mr-5"
          rounded
          color="primary"
          @click="wichButton"
          id="en"
          dark
        >
          en
        </v-btn>
        <v-btn class="mr-5"
          rounded
          color="primary"
          @click="wichButton"
          id="es"
          dark
        >
          es
        </v-btn>
        <v-btn class="mr-5"
          rounded
          color="primary"
          @click="wichButton"
          id="ja"
          dark
        >
          ja
        </v-btn>
      </v-row>
      <v-row
      justify="center"
      class="mt-10">
      <div v-if="locPopularityData.length >0" >
        <horizontal-bar :chartData="locPopularityData" :chartColors="chartColors" :options="chartOptions" :height="600" :width="900" label="Number of tweets"></horizontal-bar>
      </div>
      </v-row>
    </v-container>
</template>

<script>
import axios from 'axios';
import HorizontalBar from '../components/HorizontalBar.vue';

export default {
  name: 'LocationPopularity',
  components: {
    HorizontalBar,
  },
  data: () => ({
    locPopularityData: [
      {date: 'paris', total: 92846},
      {date: 'france', total: 75461},
      {date: 'île-de-france', total: 30130},
      {date: 'lille', total: 19576},
      {date: 'lyon', total: 19221},
      {date: 'marseille', total: 11865},
      {date: 'bruxelles', total: 9897},
      {date: 'toulouse', total: 9888},
      {date: 'strasbourg', total: 8607},
      {date: 'bordeaux', total: 8470},
    ],
    chartColors: {
      backgroundColor: "#1e72d9",
    },
    chartOptions: {
      responsive: true,
    }
  }),
  methods: {
    async wichButton(){
      let targetId = event.currentTarget.id;
      this.label = targetId;
      const url = "http://localhost:7000/location_topk/"+targetId
      let response = await axios(url)
      console.log(response)
      const days = response.data.map(day =>  day.name);
      const value = response.data.map(day => day.count);
      const result = [];
      for (let index = 0; index < 10; index++) {
        result.push({date: days[index], total: value[index]})
      }
      console.log(result);
      this.locPopularityData = [];
      await this.timeout(50);
      this.locPopularityData = result;
    },
    async timeout(ms) {
      return new Promise(resolve => setTimeout(resolve, ms));
    } 
  },
}
</script>
