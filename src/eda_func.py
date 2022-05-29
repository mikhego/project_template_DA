import pandas as pd
import plotly
from plotly import graph_objects as go

from operator import attrgetter



def retention_calculate(data, username, event_time, first_activity=None, time_period='D'):

    df_retention = data.copy()

    # задаем текущую дату события в формате time_period
    df_retention['event_date'] = df_retention[event_time].dt.to_period(time_period)

    # проверяем есть ли данные по первой активности пользователи \ находим первую активность пользователя
    if first_activity is not None:
        df_retention['cohort'] = df_retention[first_activity].dt.to_period(time_period)
    else:
        df_retention['cohort'] = df_retention.groupby(username)[event_time].transform('min').dt.to_period(time_period)

    # рассчитываем размеры когорт
    cohort_df = df_retention.groupby(['cohort', 'event_date'], as_index=False).agg(user_count=(username, 'nunique'))
    # рассчитываем lifetime события относительно первой активности пользователя
    cohort_df['lifetime'] = (cohort_df['event_date'] - cohort_df['cohort']).apply(attrgetter('n'))
    # retention по количеству пользователей
    cohort_pivot = cohort_df.pivot_table(index='cohort', columns='lifetime', values='user_count').fillna(0)
    cohort_size = cohort_pivot[0]
    # retention rate 
    retention_matrix = cohort_pivot.divide(cohort_size, axis=0).round(3)
   
    return retention_matrix 




def funnel_calculate(data, step_list, event_name, count_funnel, type_funnel='common'):
    
   df_funnel = data.copy()

   if type_funnel == 'common':

      type_graphic = 'percent initial'

      first_step = step_list[0]
      step_user_unique = df_funnel[df_funnel[event_name] == first_step][count_funnel].unique()
      first_step_df = df_funnel[df_funnel[count_funnel].isin(step_user_unique)]

      funnel_group = first_step_df.groupby(event_name).agg({count_funnel:'nunique'}).reset_index().rename(columns={count_funnel:f'{count_funnel}_count'})
      funnel_group_corr = funnel_group[funnel_group[event_name].isin(step_list)]

      dict_list = dict(zip(step_list, range(len(step_list))))

      funnel_step = funnel_group_corr.copy()
      funnel_step['step'] = funnel_step[event_name].map(dict_list)
      funnel_step = funnel_step.sort_values(by=['step']).set_index('step').reset_index()
      funnel_step['initial'] = round(funnel_step[f'{count_funnel}_count']/funnel_step[f'{count_funnel}_count'][0], 3)

   count_list = []
   if type_funnel == 'strong':

      type_graphic = 'percent previous'
      
      for step in step_list:
         step_user_unique = df_funnel[df_funnel[event_name] == step][count_funnel].unique()
         df_funnel = df_funnel[df_funnel[count_funnel].isin(step_user_unique)]
         step_user_nunique = df_funnel[count_funnel].nunique()
         
         count_list.append(int(step_user_nunique))

      funnel_tuple = list(zip(step_list,count_list))
      funnel_step = pd.DataFrame(funnel_tuple, columns=[event_name, f'{count_funnel}_count'])
      funnel_step['previous'] = funnel_step[f'{count_funnel}_count'].pct_change().fillna(0)
      funnel_step['previous'] = round(funnel_step['previous'] + 1, 3)
   

   # Построим воронку добавим на нее процент перехода 
   fig = go.Figure(
      go.Funnel(
        y=funnel_step[event_name],
        x=funnel_step[f'{count_funnel}_count'],
        textposition = "inside",
        textinfo = f"value + {type_graphic}",
         )
      )
   fig.update_layout(title=f"Воронка ({type_funnel}) событий",title_x = 0.53, autosize=False, width=1200, height=500)
   fig.show()

   return funnel_step


def sequence_target(data, session_id, event_time, event_name, target):

    show_contacts = data[data[event_name] == target].groupby(session_id).agg (contact_show_1st_time = (event_time, 'min')).reset_index()

    sequence = (
     data[[session_id, event_time, event_name]]  
    .assign(is_contacts_show = data[event_name] == target) # разметим событие "contacts_show" в каждой сессии
    .merge(show_contacts, how='left', on=session_id)
    )   
    # добавлю колонку с подсчетом количества событий 'contacts_show' в сессии
    sequence['contacts_show_per_session'] = sequence.groupby(session_id)['is_contacts_show'].transform('sum')

# разметим длительность событий
    sequence['delta_sec'] = (
        sequence.groupby(session_id)[event_time].diff()
        .fillna(pd.Timedelta(minutes=0))
        .astype("timedelta64[s]")
        .astype('int32')
        ) + 1 #добавлю 1 секунду, чтобы первое событие не было длительностью 0 и его было видно на графике

    sequence['session_start'] = sequence.groupby(session_id)[event_time].transform('min')
    sequence['session_opened_contacts'] = sequence.groupby(session_id)['is_contacts_show'].transform('sum') > 0
    sequence['after_1st_contact'] = (sequence[event_name].eq(target)).groupby(sequence[session_id]).cumsum() > 0


    order = sequence.query('session_opened_contacts == True')[[session_id, event_time, event_name, 'contact_show_1st_time']]
              
    order['rank'] = order.groupby(session_id).cumcount()

    contact_show_position = order[order[event_name] == target][[session_id,'rank']].rename(columns={'rank':'contact_show_position'})
    contact_show_position = contact_show_position.groupby(session_id).agg({'contact_show_position':'first'})

    order = (order.merge(contact_show_position, on=session_id, how='left').assign(delta = lambda x: (x['contact_show_position'] - x['rank']) * -1))


    return sequence, order



