import Vue from 'vue'
import VueRouter from 'vue-router'
import Home from '../views/Home.vue'
import WordEvo from '../views/WordEvolution.vue';
import HashtagEvo from '../views/HashtagEvolution.vue'
import LangEvo from '../views/LangEvolution.vue'
import LangPopu from '../views/LangPopularity.vue'
import LocationPopularity from '../views/LocPopularity.vue'

Vue.use(VueRouter)

const routes = [
  {
    path: '/',
    name: 'Home',
    component: Home
  },
  {
    path: '/wordevo',
    name: 'WordEvolution',
    // route level code-splitting
    // this generates a separate chunk (about.[hash].js) for this route
    // which is lazy-loaded when the route is visited.
    component: WordEvo
  },
  {
    path: '/hashtagevo',
    name: 'HashtagEvolution',
    component: HashtagEvo
  },
  {
    path: '/langevo',
    name: 'LangEvolution',
    component: LangEvo
  },
  {
    path: '/langpop',
    name: 'LangPopularity',
    component: LangPopu
  },
  {
    path: '/locpop',
    name: 'LocationPopularity',
    component: LocationPopularity
  },
]

const router = new VueRouter({
  routes
})

export default router
